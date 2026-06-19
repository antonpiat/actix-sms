use actix_cors::Cors;
use actix_web::{get, post, put, web, App, HttpResponse, HttpServer, Responder};
use actix_web_actors::ws;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use mongodb::{
    bson::{doc, oid::ObjectId},
    options::ClientOptions,
    Client, Collection,};
use redis::{AsyncCommands, Client as RedisClient};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use uuid::Uuid;

use actix::{Actor, ActorContext, AsyncContext, Handler, Message as ActixMessage, StreamHandler};

// Custom message type for WebSocket communication
#[derive(ActixMessage)]
#[rtype(result = "()")]
struct WsMessage(String);

// Websocket connection state
struct WebSocketSession {
    id: Uuid,
    hb: Instant,
}

impl WebSocketSession {
    fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            hb: Instant::now(),
        }
    }

    fn hb(&self) -> Duration {
        Instant::now().duration_since(self.hb)
    }
}

impl Actor for WebSocketSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(10), |act, ctx| {
            if act.hb() > Duration::from_secs(30) {
                println!("WebSocket connection dropped");
                ctx.stop();
            } else {
                ctx.ping(b"");
            }
        });
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocketSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(text)) => {
                println!("Received WebSocket message: {}", text);
                ctx.text(text);
            }
            _ => (),
        }
    }
}

impl Handler<WsMessage> for WebSocketSession {
    type Result = ();
    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct ChatMessage {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<ObjectId>,
    text: String,
    author: String,
    timestamp: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct UpdateMessage {
    text: String,
}

type WebSocketSessions = Arc<RwLock<HashMap<Uuid, actix::Addr<WebSocketSession>>>>;

struct AppState {
    mongo: Collection<ChatMessage>,
    redis_client: Arc<RedisClient>,
    ws_sessions: WebSocketSessions,
}

/// WebSocket handler
#[get("/ws")]
async fn websocket(
    req: actix_web::HttpRequest,
    stream: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let session = WebSocketSession::new();
    let session_id = session.id;
    
    let resp = ws::WsResponseBuilder::new(session, &req, stream).start_with_addr()?;
    
    let (addr, response) = resp;
    
    // Store the seesion for broadcasting
    {
        let mut session_write = data.ws_sessions.write().await;
        session_write.insert(session_id, addr.clone());
        println!("New websocket connection established with id {}", session_id);
    }
    
    // Remove session when connection closes
    let session_clone = data.ws_sessions.clone();
    
    actix_rt::spawn(async move {
        println!("WebSocket session {} started", session_id);
        
        tokio::time::sleep(Duration::from_secs(3600)).await;
        
        let mut session_write = session_clone.write().await;
        session_write.remove(&session_id);
        println!("WebSocket session {} removed", session_id);
    });
    
    Ok(response)
}

/// Broadcast message to all WebSocket clients
async fn broadcast_to_websocket(sessions: &WebSocketSessions, message: &str) {
    let sessions_read = sessions.read().await;
    let mut disconnected_sessions = Vec::new();
    
    for (id, addr) in sessions_read.iter() {
        match addr.try_send(WsMessage(message.to_string())) {
            Ok(_) => (),
            Err(_) => disconnected_sessions.push(*id),
        }
    }
    
    if !disconnected_sessions.is_empty() {
        drop(sessions_read);
        let mut session_write = sessions.write().await;
        for id in disconnected_sessions {
            session_write.remove(&id);
            println!("Cleaned up disconnected websocket session {}", id);
        }
    }
}

/// SSE endpoint (for clients that prefer SSE)
#[get("/events")]
async fn sse_events(data: web::Data<AppState>) -> impl Responder {
    let (mut tx, rx) = futures::channel::mpsc::channel::<String>(100);
    let ws_sessions = data.ws_sessions.clone();
    
    actix_rt::spawn(async move {
       let pubsub_client = RedisClient::open("redis://127.0.0.1/").unwrap(); 
        let mut pubsub = match pubsub_client.get_async_pubsub().await {
            Ok(connection) => connection,
            Err(e) => {
                println!("Failed to create pubsub Redis connection: {}", e);
                return 
            }
        };
        
        if let Err(e) = pubsub.subscribe("updates").await {
            eprintln!("Failed to subscribe to updates: {}", e);
            return 
        }
        
        while let Some(msg) = pubsub.on_message().next().await {
            let payload: String = match msg.get_payload() {
                Ok(payload) => payload,
                Err(e) => {
                    eprintln!("Failed to get payload: {}", e);
                    continue
                }
            };
            
            if tx.send(payload.clone()).await.is_err() {
                break;
            }
            
            broadcast_to_websocket(&ws_sessions, &payload).await;
        };
    });

    let stream = rx.map(|msg| Ok::<_, actix_web::Error>(Bytes::from(format!("data: {}\n\n", msg))));
    
    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("Access-Control-Allow-Origin", "*"))
        .streaming(stream)
}

/// Post new message
#[post("/publish")]
async fn publish(state: web::Data<AppState>, payload: web::Json<ChatMessage>) -> impl Responder {
    let mut msg = payload.into_inner();
    
    msg.timestamp = Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    
    let insert_result = match state.mongo.insert_one(&msg).await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Failed to insert new document into MongoDB: {}", e);
            return HttpResponse::InternalServerError().body("Failed to save message")
        }
    };
    msg.id = insert_result.inserted_id.as_object_id();
    
    let mut connection = match state.redis_client.get_multiplexed_async_connection().await {
        Ok(connection) => connection,
        Err(e) => {
            eprintln!("Failed to get Redis multiplexed async connection: {}", e);
            return HttpResponse::InternalServerError().body("Failed to connect to Redis");
        }
    };
    
    let json_msg = match serde_json::to_string(&msg) {
        Ok(json_msg) => json_msg,
        Err(e) => {
            eprintln!("Failed to serialize message: {}", e);
            return HttpResponse::InternalServerError().body("Failed to serialize message");
        }
    };
    
    let result: Result<(), redis::RedisError> = connection.publish("updates", &json_msg).await;
    if let Err(e) = result {
        eprintln!("Failed to publish message: {}", e);
        return HttpResponse::InternalServerError().body("Failed to publish message");
    }

    HttpResponse::Ok().json(msg)
}

/// Put edit message
#[put("/edit/{id}")]
async fn edit_message(
    state: web::Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<UpdateMessage>,
) -> impl Responder {
    let id_str = path.into_inner();
    
    let id = match ObjectId::parse_str(&id_str) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("Invalid ObjectId: '{}': {}", id_str, e);
            return HttpResponse::BadRequest().body(format!("Invalid message ID: {}", id_str));
        }
    };
    
    let filter = doc! {"_id": id};
    let update = doc! {"$set": {"text": &payload.text}};
    
    let update_result = match state.mongo.update_one(filter.clone(), update).await {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Failed to update message: {}", e);
            return HttpResponse::InternalServerError().body("Failed to update message");
        }
    };
    
    if update_result.matched_count == 0 {
        return HttpResponse::NotFound().body("Message not found");
    }
    
    let mut updated = match state.mongo.find_one(filter).await {
        Ok(Some(msg)) => msg,
        Ok(None) => {
            return HttpResponse::NotFound().body("Message not found after update");
        }
        Err(e) => {
            eprintln!("Failed to fetch updated message: {}", e);
            return HttpResponse::InternalServerError().body("Failed to fetch updated message");
        }
    };
    
    updated.timestamp = Some(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    
    let mut connection = match state.redis_client.get_multiplexed_async_connection().await {
        Ok(connection) => connection,
        Err(e) => {
            eprintln!("Failed to connect to Redis: {}", e);
            return HttpResponse::InternalServerError().body("Failed to connect to Redis");
        }
    };
    
    let json_msg = match serde_json::to_string(&updated) {
        Ok(json_msg) => json_msg,
        Err(e) => {
            eprintln!("Failed to serialize message: {}",e);
            return HttpResponse::InternalServerError().body("Failed to serialize message");
        }
    };
    
    let result: Result<(), redis::RedisError> = connection.publish("updates", &json_msg).await;
    if let Err(e) = result {
        eprintln!("Failed to publish updated message: {}", e);
        return HttpResponse::InternalServerError().body("Failed to publish updated message");
    }
    
    HttpResponse::Ok().json(updated)
}

/// Get all messages (for new WebSocket clients)
#[get("/messages")]
async fn get_messages(state: web::Data<AppState>) -> impl Responder {
    match state.mongo.find(doc! {}).await {
        Ok(mut cursor) => {
            let mut messages = Vec::new();
            while let Some(Ok(message)) = cursor.next().await {
                messages.push(message);
            }
            HttpResponse::Ok().json(messages)
        }
        Err(e) => {
            eprintln!("Failed to fetch messages: {}", e);
            HttpResponse::InternalServerError().body("Failed to fetch messages")
        }
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // MongoDB Setup
    let client_options  = match ClientOptions::parse("mongodb://localhost:27017").await {
        Ok(options) => options,
        Err(e) => {
            eprintln!("Failed to parse MongoDB connection string: {}", e);
            std::process::exit(1);
        }
    };
    
    let client = match Client::with_options(client_options) {
        Ok(client) => client,
        Err(e) => {
            eprintln!("Failed to create MongoDB client: {}", e);
            std::process::exit(1);
        }
    };
    
    let db = client.database("chat");
    let collection = db.collection::<ChatMessage>("messages");
    
    // Redis setup
    let redis_client = match RedisClient::open("redis://localhost") {
        Ok(redis_client) => redis_client,
        Err(e) => {
            eprintln!("Failed to create redis client: {}", e);
            std::process::exit(1);
        }
    };
    
    // Test Redis connection
    match redis_client.get_multiplexed_async_connection().await {
        Ok(_) => println!("Connected to Redis successfully"),
        Err(e) => {
            eprintln!("Failed to connect to Redis: {}", e);
            std::process::exit(1);
        }
    }
    
    let state = web::Data::new(AppState {
        mongo: collection,
        redis_client: Arc::new(redis_client),
        ws_sessions: Arc::new(RwLock::new(HashMap::new())),
    });

    println!("🚀 Server running on http://127.0.0.1:8080");
    println!("📡 SSE endpoint: http://127.0.0.1:8080/events");
    println!("🔌 WebSocket endpoint: ws://127.0.0.1:8080/ws");
    println!("📨 Messages endpoint: http://127.0.0.1:8080/messages");
    
    HttpServer::new(move || {
        App::new()
            .wrap(
                Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header()
                    .supports_credentials(),
            )
            .app_data(state.clone())
            .service(sse_events)
            .service(websocket)
            .service(publish)
            .service(edit_message)
            .service(get_messages)
    })
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
