'use client';
import { useEffect, useRef, useState } from 'react';

interface Message {
    _id?: string;
    author: string;
    text: string;
    timestamp?: number;
}

export default function WebSocketPage() {
    const [messages, setMessages] = useState<Message[]>([]);
    const [text, setText] = useState('');
    const [author, setAuthor] = useState('');
    const [editingId, setEditingId] = useState<string | null>(null);
    const [editText, setEditText] = useState('');
    const [isConnected, setIsConnected] = useState(false);
    const ws = useRef<WebSocket | null>(null);
    const messagesEndRef = useRef<HTMLDivElement>(null);
    const messagesContainerRef = useRef<HTMLDivElement>(null);
    const messageIds = useRef<Set<string>>(new Set());
    
    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    // Auto-scroll to bottom when new messages arrive
    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    function setupWebSocket() {
        ws.current = new WebSocket('ws://localhost:8080/ws');

        ws.current.onopen = () => {
            console.log('WebSocket connected');
            setIsConnected(true);
        };

        ws.current.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data) as Message;
                console.log('WebSocket received:', msg);
                handleNewMessage(msg);
            } catch (error) {
                console.error('Failed to parse WebSocket message:', error);
            }
        };

        ws.current.onclose = () => {
            console.log('WebSocket disconnected');
            setIsConnected(false);
        };

        ws.current.onerror = (error) => {
            console.error('WebSocket error:', error);
            setIsConnected(false);
        };
    }


    function handleNewMessage(msg: Message) {
        setMessages((prev) => {
            // If message has no ID, add it directly
            if (!msg._id) {
                return [...prev, msg];
            }

            // Check if we've already seen this message ID
            if (messageIds.current.has(msg._id)) {
                console.log('Duplicate message detected, skipping:', msg._id);
                return prev;
            }

            // Add the message ID to our tracking set
            messageIds.current.add(msg._id);

            // Check if message already exists by ID
            const existingIndex = prev.findIndex((m) => m._id === msg._id);
            if (existingIndex !== -1) {
                // Update existing message
                const newArr = [...prev];
                newArr[existingIndex] = msg;
                return newArr;
            }

            // Add new message
            return [...prev, msg];
        });
    }


    useEffect(() => {
        async function loadMessages() {
            try {
                const response = await fetch('http://localhost:8080/messages');
                const messages = await response.json();
                setMessages(messages);
                // Store existing message IDs to prevent duplicates
                messages.forEach((msg: Message) => {
                    if (msg._id) {
                        messageIds.current.add(msg._id);
                    }
                });
            } catch (error) {
                console.error('Failed to load messages:', error);
            }
        }
        
        loadMessages();
        
        setupWebSocket();

        return () => {
            if (ws.current) {
                ws.current.close();
            }
        };
    }, []);

    async function sendMessage() {
        if (!text.trim() || !author.trim()) return;

        const messageText = text.trim();
        const messageAuthor = author.trim();

        try {
            // Option 1: Send only via HTTP POST (recommended to avoid duplicates)
            const response = await fetch('http://localhost:8080/publish', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    text: messageText,
                    author: messageAuthor,
                }),
            });

            if (response.ok) {
                setText('');
                console.log('Message sent successfully via HTTP POST');
            } else {
                console.error('Failed to send message:', await response.text());
            }

            // Option 2: If you want to use WebSocket for sending, remove the HTTP POST above
            // and uncomment the code below:
            /*
            if (ws.current?.readyState === WebSocket.OPEN) {
              // Create a temporary ID for optimistic update
              const tempId = `temp-${Date.now()}`;
              const optimisticMessage: Message = {
                _id: tempId,
                text: messageText,
                author: messageAuthor,
                timestamp: Math.floor(Date.now() / 1000)
              };
      
              // Add optimistic update
              setMessages(prev => [...prev, optimisticMessage]);
              setText('');
      
              // Send via WebSocket
              ws.current.send(JSON.stringify({
                type: 'message',
                text: messageText,
                author: messageAuthor
              }));
              console.log('Message sent via WebSocket');
            } else {
              // Fallback to HTTP POST if WebSocket is not connected
              const response = await fetch('http://localhost:8080/publish', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ text: messageText, author: messageAuthor }),
              });
              if (response.ok) {
                setText('');
              }
            }
            */
        } catch (error) {
            console.error('Failed to send message:', error);
        }
    }

    async function saveEdit(id: string) {
        await fetch(`http://localhost:8080/edit/${id}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: editText }),
        });
        setEditingId(null);
        setEditText('');
    }

    function formatTimestamp(timestamp?: number) {
        if (!timestamp) return '';
        return new Date(timestamp * 1000).toLocaleTimeString();
    }

    return (
        <div className="min-h-screen max-h-screen flex flex-col bg-base-200">
            {/* Header - Fixed */}
            {/* <div className="bg-info/10 backdrop-blur-lg text-primary-content px-4 py-2 shadow-lg flex-shrink-0">
        <div className="max-w-4xl mx-auto flex items-center justify-between">
          <div className="flex items-center space-x-4">
            <div className="avatar">
              <div className="w-10 h-10 rounded-full bg-secondary flex items-center justify-center">
                <span className="text-lg">💬</span>
              </div>
            </div>
            <div>
              <h1 className="text-xl font-bold">WebSocket Messenger</h1>
              <div className="flex items-center space-x-2 text-sm">
                <div
                  className={`w-2 h-2 rounded-full ${isConnected ? 'bg-success' : 'bg-error'}`}
                ></div>
                <span>{isConnected ? 'Connected' : 'Disconnected'}</span>
              </div>
            </div>
          </div>
          <div className="badge badge-secondary">WebSocket</div>
        </div>
      </div> */}

            {/* Main Content Area - Flex container that takes remaining space */}
            <div className="flex-1 flex flex-col max-w-4xl mx-auto w-full px-4 min-h-0">
                {/* Messages Container - Scrollable area */}
                <div ref={messagesContainerRef} className="flex-1 overflow-y-auto space-y-4 mb-4 min-h-0">
                    {messages.length === 0 ? (
                        <div className="flex items-center justify-center h-full text-base-content/50">
                            <div className="text-center">
                                <div className="text-6xl mb-4">💬</div>
                                <p className="text-lg">No messages yet</p>
                                <p className="text-sm">Start a conversation by sending a message</p>
                            </div>
                        </div>
                    ) : (
                        messages.map((message) => (
                            <div
                                key={message._id}
                                className={`chat ${message.author === author ? 'chat-end' : 'chat-start'}`}
                            >
                                <div className="chat-image avatar">
                                    <div className="w-8 h-8 rounded-full bg-neutral flex items-center justify-center">
                                        <span className="text-xs">{message.author.charAt(0).toUpperCase()}</span>
                                    </div>
                                </div>
                                <div className="chat-header mb-1">
                                    {message.author}
                                    <time className="text-xs opacity-50 ml-2">
                                        {formatTimestamp(message.timestamp)}
                                    </time>
                                </div>
                                <div className="chat-bubble bg-base-300 text-base-content relative group">
                                    {editingId === message._id ? (
                                        <div className="flex space-x-2">
                                            <input
                                                className="input input-bordered input-sm flex-1"
                                                value={editText}
                                                onChange={(e) => setEditText(e.target.value)}
                                                autoFocus
                                                onKeyPress={(e) => e.key === 'Enter' && saveEdit(message._id!)}
                                            />
                                            <button
                                                className="btn btn-success btn-sm"
                                                onClick={() => saveEdit(message._id!)}
                                            >
                                                ✓
                                            </button>
                                            <button className="btn btn-ghost btn-sm" onClick={() => setEditingId(null)}>
                                                ✕
                                            </button>
                                        </div>
                                    ) : (
                                        <>
                                            <span>{message.text}</span>
                                            {message.author === author && (
                                                <button
                                                    className="btn btn-xs btn-ghost opacity-0 group-hover:opacity-100 ml-2 transition-opacity"
                                                    onClick={() => {
                                                        setEditingId(message._id || null);
                                                        setEditText(message.text);
                                                    }}
                                                >
                                                    ✏️
                                                </button>
                                            )}
                                        </>
                                    )}
                                </div>
                            </div>
                        ))
                    )}
                    {/* Invisible element at the bottom for auto-scrolling */}
                    <div ref={messagesEndRef} />
                </div>

                {/* Input Area - Fixed at bottom */}
                <div className="bg-base-100 rounded-lg shadow-lg p-4 flex-shrink-0">
                    <div className="flex space-x-2 mb-2">
                        <input
                            type="text"
                            placeholder="Your Name"
                            value={author}
                            onChange={(e) => setAuthor(e.target.value)}
                            className="input input-bordered flex-1"
                        />
                    </div>
                    <div className="flex space-x-2">
                        <input
                            type="text"
                            placeholder="Type a message..."
                            value={text}
                            onChange={(e) => setText(e.target.value)}
                            onKeyPress={(e) => e.key === 'Enter' && sendMessage()}
                            className="input input-bordered flex-1"
                        />
                        <button
                            onClick={sendMessage}
                            className="btn btn-primary"
                            disabled={!text.trim() || !author.trim()}
                        >
                            Send
                        </button>
                    </div>
                </div>
            </div>
        </div>
    );
}