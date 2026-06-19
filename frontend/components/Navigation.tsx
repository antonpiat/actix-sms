'use client'

import {usePathname} from "next/navigation";
import Link from "next/link";

export default function Navigation() {
    const pathname = usePathname();

    return (
        <div className="navbar bg-base-100 shadow-lg">
            <div className="navbar-start">
                <Link href="/" className="btn btn-ghost text-xl">
                    💬 Messenger Demo
                </Link>
            </div>
            <div className="navbar-center">
                <div className="tabs">
                    <Link href="/websocket" className={`tab tab-lg tab-bordered ${pathname === '/websocket' ? 'tab-active' : ''}`}>
                        🔌 WebSocket
                    </Link>
                    <Link href="/sse" className={`tab tab-lg tab-bordered ${pathname === '/sse' ? 'tab-active' : ''}`}>
                        📡 SSE
                    </Link>
                </div>
            </div>
        </div>
    )
}