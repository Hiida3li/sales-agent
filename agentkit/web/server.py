"""FastAPI web chat server.

Serves a single-page chat UI and a ``POST /api/chat`` endpoint. Each request
is published to ``agent-requests``; the endpoint then waits on
``respond_to_user`` for the reply whose header id matches the one it sent.
Per-session conversation context is kept in memory so multi-turn chats
accumulate history, mirroring what the CLI does.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel

from agentkit.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

REQUESTS_TOPIC = "agent-requests"
RESPONSE_TOPIC = "respond_to_user"
REPLY_TIMEOUT_SECONDS = 60
INIT_PAYLOAD_FILE = "init_payload.json"

DEFAULT_PROMPT = (
    "You are a helpful e-commerce customer service agent. Use the available "
    "tools to find products and answer policy questions.\n\n"
    "--- Conversation History ---\n${history}\n\n--- User Query ---\n${query}"
)


def _base_payload() -> Dict[str, Any]:
    """Load the request template from init_payload.json, or fall back."""
    if os.path.exists(INIT_PAYLOAD_FILE):
        with open(INIT_PAYLOAD_FILE) as f:
            return json.load(f)
    return {
        "header": {"id": "", "timestamp": ""},
        "payload": {
            "agent": {
                "context": {
                    "allowed_tools": ["search_products", "search_faqs", "respond_to_user"],
                    "query": "",
                    "history": [],
                },
                "prompt": DEFAULT_PROMPT,
            }
        },
    }


def _extract_answer(message: Dict[str, Any]) -> Optional[str]:
    agent = message.get("payload", {}).get("agent", {})
    current = agent.get("current_function_execution", {})
    if current.get("name") == "respond_to_user":
        return current.get("args", {}).get("content", "")
    return None


class KafkaBridge:
    """Publishes a query and blocks until the correlated reply arrives."""

    def __init__(self, broker: str):
        self.broker = broker
        self.producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        # session_id -> last full payload, so context accumulates across turns.
        self._sessions: Dict[str, Dict[str, Any]] = {}

    def _payload_for(self, session_id: str, query: str) -> Dict[str, Any]:
        payload = self._sessions.get(session_id) or _base_payload()
        agent = payload["payload"]["agent"]
        # Reset transient routing fields from a previous turn before resending.
        for key in ("current_function_execution", "remaining_function_calls", "function_call"):
            agent.pop(key, None)
        agent["context"]["query"] = query
        payload["header"]["id"] = f"web-{session_id}-{int(time.time() * 1000)}"
        payload["header"]["timestamp"] = str(time.time())
        return payload

    def ask(self, session_id: str, query: str) -> str:
        payload = self._payload_for(session_id, query)
        correlation_id = payload["header"]["id"]

        # Subscribe before producing so the reply (latest offset) is not missed.
        consumer = KafkaConsumer(
            RESPONSE_TOPIC,
            bootstrap_servers=[self.broker],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=f"web-bridge-{uuid.uuid4()}",
        )
        deadline = time.time() + 15
        while not consumer.assignment() and time.time() < deadline:
            consumer.poll(timeout_ms=200)

        self.producer.send(REQUESTS_TOPIC, payload).get(timeout=10)
        logger.info(f"Sent query for session {session_id}: {query!r}")

        deadline = time.time() + REPLY_TIMEOUT_SECONDS
        try:
            while time.time() < deadline:
                for _, records in consumer.poll(timeout_ms=1000).items():
                    for record in records:
                        if record.value.get("header", {}).get("id") != correlation_id:
                            continue
                        answer = _extract_answer(record.value)
                        if answer:
                            self._sessions[session_id] = record.value
                            return answer
        finally:
            consumer.close()

        return "The agent did not respond in time. Please try again."


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None


def create_app() -> FastAPI:
    app = FastAPI(title="Agent Chat")
    bridge = KafkaBridge(settings.kafka_broker)

    @app.get("/health")
    def health() -> Dict[str, str]:
        return {"status": "ok"}

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

    @app.post("/api/chat")
    def chat(req: ChatRequest) -> JSONResponse:
        session_id = req.session_id or str(uuid.uuid4())
        answer = bridge.ask(session_id, req.message)
        return JSONResponse({"reply": answer, "session_id": session_id})

    return app


app = create_app()


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Aura — Shopping Assistant</title>
<style>
  :root {
    --bg: #f6f7f9;
    --panel: #ffffff;
    --panel-2: #f1f3f6;
    --text: #1a1d24;
    --text-soft: #5b6473;
    --text-faint: #9aa3b2;
    --border: #e5e8ee;
    --accent: #4f46e5;
    --accent-strong: #4338ca;
    --accent-soft: #eef0fe;
    --user-bubble: linear-gradient(135deg, #4f46e5, #6366f1);
    --shadow: 0 1px 2px rgba(16,24,40,.06), 0 8px 24px rgba(16,24,40,.06);
    --radius: 18px;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0c0e13;
      --panel: #14171f;
      --panel-2: #1b1f29;
      --text: #e8eaf0;
      --text-soft: #a4adbd;
      --text-faint: #6b7383;
      --border: #232834;
      --accent: #7c83ff;
      --accent-strong: #9aa0ff;
      --accent-soft: #1c2030;
      --shadow: 0 1px 2px rgba(0,0,0,.4), 0 12px 32px rgba(0,0,0,.35);
    }
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; }
  body {
    margin: 0;
    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    background: var(--bg);
    color: var(--text);
    -webkit-font-smoothing: antialiased;
    display: flex;
    justify-content: center;
  }
  .app {
    width: 100%;
    max-width: 860px;
    height: 100dvh;
    display: flex;
    flex-direction: column;
    background: var(--panel);
    border-left: 1px solid var(--border);
    border-right: 1px solid var(--border);
  }

  /* Header */
  header {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 14px 20px;
    background: var(--panel);
    border-bottom: 1px solid var(--border);
    position: sticky; top: 0; z-index: 5;
  }
  .brand-logo {
    width: 38px; height: 38px; border-radius: 11px;
    background: linear-gradient(135deg, #4f46e5, #8b5cf6);
    display: grid; place-items: center;
    color: #fff; font-weight: 700; font-size: 18px;
    box-shadow: 0 4px 12px rgba(79,70,229,.35);
    flex-shrink: 0;
  }
  .brand-text { display: flex; flex-direction: column; line-height: 1.2; }
  .brand-text .name { font-weight: 650; font-size: 15px; letter-spacing: .2px; }
  .brand-text .status { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-soft); }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: #9aa3b2; transition: background .3s; }
  .dot.online { background: #22c55e; box-shadow: 0 0 0 3px rgba(34,197,94,.18); }
  .dot.offline { background: #ef4444; box-shadow: 0 0 0 3px rgba(239,68,68,.18); }
  .spacer { flex: 1; }
  .ghost-btn {
    appearance: none; border: 1px solid var(--border); background: transparent;
    color: var(--text-soft); font-size: 13px; font-weight: 550;
    padding: 8px 14px; border-radius: 10px; cursor: pointer; display: flex; align-items: center; gap: 6px;
    transition: all .15s;
  }
  .ghost-btn:hover { background: var(--panel-2); color: var(--text); }

  /* Message log */
  #log {
    flex: 1; overflow-y: auto; padding: 24px 20px 8px;
    display: flex; flex-direction: column; gap: 18px;
    scroll-behavior: smooth;
  }
  .row { display: flex; gap: 12px; align-items: flex-end; max-width: 100%; animation: rise .28s ease both; }
  .row.user { flex-direction: row-reverse; }
  @keyframes rise { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: none; } }
  .avatar {
    width: 30px; height: 30px; border-radius: 9px; flex-shrink: 0;
    display: grid; place-items: center; font-size: 13px; font-weight: 700; color: #fff;
    margin-bottom: 2px;
  }
  .avatar.bot { background: linear-gradient(135deg, #4f46e5, #8b5cf6); }
  .avatar.me  { background: var(--panel-2); color: var(--text-soft); border: 1px solid var(--border); }
  .bubble-wrap { display: flex; flex-direction: column; max-width: 76%; }
  .row.user .bubble-wrap { align-items: flex-end; }
  .bubble {
    padding: 11px 15px; border-radius: var(--radius); line-height: 1.5; font-size: 14.5px;
    word-wrap: break-word; box-shadow: var(--shadow);
  }
  .row.agent .bubble {
    background: var(--panel); border: 1px solid var(--border);
    border-bottom-left-radius: 5px; color: var(--text);
  }
  .row.user .bubble {
    background: var(--user-bubble); color: #fff; border-bottom-right-radius: 5px; box-shadow: 0 6px 18px rgba(79,70,229,.28);
  }
  .bubble p { margin: 0 0 8px; } .bubble p:last-child { margin-bottom: 0; }
  .bubble ul { margin: 6px 0; padding-left: 20px; } .bubble li { margin: 2px 0; }
  .bubble code { background: var(--panel-2); padding: 1px 6px; border-radius: 6px; font-size: 13px;
                 font-family: ui-monospace, "SF Mono", Menlo, monospace; }
  .row.user .bubble code { background: rgba(255,255,255,.18); }
  .time { font-size: 11px; color: var(--text-faint); margin: 4px 6px 0; }

  /* Typing indicator */
  .typing { display: flex; gap: 4px; padding: 14px 16px; }
  .typing span {
    width: 7px; height: 7px; border-radius: 50%; background: var(--text-faint);
    animation: blink 1.3s infinite ease-in-out both;
  }
  .typing span:nth-child(2) { animation-delay: .18s; }
  .typing span:nth-child(3) { animation-delay: .36s; }
  @keyframes blink { 0%, 80%, 100% { opacity: .25; transform: translateY(0); }
                     40% { opacity: 1; transform: translateY(-3px); } }

  /* Welcome / empty state */
  .welcome { margin: auto; text-align: center; max-width: 520px; padding: 24px 0; animation: rise .4s ease both; }
  .welcome .hero {
    width: 60px; height: 60px; border-radius: 18px; margin: 0 auto 18px;
    background: linear-gradient(135deg, #4f46e5, #8b5cf6); display: grid; place-items: center;
    color: #fff; font-size: 28px; font-weight: 700; box-shadow: 0 10px 30px rgba(79,70,229,.4);
  }
  .welcome h1 { font-size: 22px; margin: 0 0 8px; letter-spacing: -.2px; }
  .welcome p { color: var(--text-soft); margin: 0 0 22px; font-size: 14.5px; line-height: 1.55; }
  .chips { display: flex; flex-wrap: wrap; gap: 10px; justify-content: center; }
  .chip {
    border: 1px solid var(--border); background: var(--panel); color: var(--text);
    padding: 10px 14px; border-radius: 12px; font-size: 13.5px; cursor: pointer; text-align: left;
    transition: all .15s; box-shadow: var(--shadow);
  }
  .chip:hover { border-color: var(--accent); background: var(--accent-soft); transform: translateY(-1px); }
  .chip .k { display: block; font-weight: 600; margin-bottom: 1px; }
  .chip .s { display: block; color: var(--text-faint); font-size: 12px; }

  /* Composer */
  .composer-wrap { padding: 12px 16px 18px; background: var(--panel); border-top: 1px solid var(--border); }
  .composer {
    display: flex; align-items: flex-end; gap: 10px;
    background: var(--panel-2); border: 1px solid var(--border); border-radius: 16px;
    padding: 8px 8px 8px 16px; transition: border-color .15s, box-shadow .15s;
  }
  .composer:focus-within { border-color: var(--accent); box-shadow: 0 0 0 4px var(--accent-soft); }
  textarea {
    flex: 1; border: 0; background: transparent; resize: none; outline: none;
    color: var(--text); font: inherit; font-size: 15px; line-height: 1.5;
    max-height: 160px; padding: 7px 0;
  }
  textarea::placeholder { color: var(--text-faint); }
  .send {
    width: 40px; height: 40px; flex-shrink: 0; border: 0; border-radius: 12px; cursor: pointer;
    background: var(--accent); color: #fff; display: grid; place-items: center; transition: all .15s;
  }
  .send:hover:not(:disabled) { background: var(--accent-strong); }
  .send:disabled { opacity: .4; cursor: default; }
  .send svg { width: 18px; height: 18px; }
  .hint { text-align: center; font-size: 11.5px; color: var(--text-faint); margin: 8px 0 0; }

  #log::-webkit-scrollbar { width: 10px; }
  #log::-webkit-scrollbar-thumb { background: var(--border); border-radius: 8px; border: 3px solid var(--panel); }

  @media (max-width: 640px) {
    .app { border: 0; }
    .bubble-wrap { max-width: 84%; }
  }
</style>
</head>
<body>
  <div class="app">
    <header>
      <div class="brand-logo">A</div>
      <div class="brand-text">
        <span class="name">Aura</span>
        <span class="status"><span id="dot" class="dot"></span><span id="statusText">connecting…</span></span>
      </div>
      <div class="spacer"></div>
      <button id="newChat" class="ghost-btn" title="Start a new conversation">
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
             stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14M5 12h14"/></svg>
        New chat
      </button>
    </header>

    <div id="log" aria-live="polite"></div>

    <div class="composer-wrap">
      <form id="form" class="composer">
        <textarea id="input" rows="1" placeholder="Ask about products, shipping, returns…" autofocus
                  aria-label="Message"></textarea>
        <button id="send" class="send" type="submit" disabled aria-label="Send message">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round"><path d="M22 2 11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>
        </button>
      </form>
      <p class="hint">Enter to send &middot; Shift + Enter for a new line</p>
    </div>
  </div>

<script>
  const log = document.getElementById('log');
  const form = document.getElementById('form');
  const input = document.getElementById('input');
  const send = document.getElementById('send');
  const dot = document.getElementById('dot');
  const statusText = document.getElementById('statusText');
  let sessionId = null;
  let busy = false;

  const SUGGESTIONS = [
    { k: 'Find a product', s: 'Do you have the iPhone 15 Pro in red?' },
    { k: 'Check availability', s: 'Which phones are currently in stock?' },
    { k: 'Shipping & delivery', s: 'How long does shipping take?' },
    { k: 'Returns & refunds', s: 'What is your return policy?' },
  ];

  function escapeHtml(s) {
    return s.replace(/[&<>"']/g, c => ({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;' }[c]));
  }
  // Minimal, safe markdown: escape first, then inline + lists + paragraphs.
  function format(text) {
    const lines = escapeHtml(text).split('\n');
    let html = '', list = false;
    const inline = t => t
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/`([^`]+?)`/g, '<code>$1</code>')
      .replace(/(?<!\*)\*(?!\*)(.+?)\*(?!\*)/g, '<em>$1</em>');
    for (let raw of lines) {
      const line = raw.trim();
      if (/^[-*]\s+/.test(line)) {
        if (!list) { html += '<ul>'; list = true; }
        html += '<li>' + inline(line.replace(/^[-*]\s+/, '')) + '</li>';
      } else {
        if (list) { html += '</ul>'; list = false; }
        if (line) html += '<p>' + inline(line) + '</p>';
      }
    }
    if (list) html += '</ul>';
    return html || '<p></p>';
  }

  function now() {
    return new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }

  function renderWelcome() {
    log.innerHTML = '';
    const w = document.createElement('div');
    w.className = 'welcome';
    w.innerHTML =
      '<div class="hero">A</div>' +
      '<h1>How can I help you today?</h1>' +
      '<p>I\'m Aura, your AI shopping assistant. I can find items in our ' +
      'catalog and answer questions about shipping, returns, payment, and ' +
      'warranty. Pick a starting point below or type your own.</p>' +
      '<div class="chips"></div>';
    const chips = w.querySelector('.chips');
    SUGGESTIONS.forEach(({ k, s }) => {
      const c = document.createElement('button');
      c.className = 'chip';
      c.type = 'button';
      c.innerHTML = '<span class="k">' + k + '</span><span class="s">' + s + '</span>';
      c.addEventListener('click', () => { if (!busy) { input.value = s; resize(); submit(); } });
      chips.appendChild(c);
    });
    log.appendChild(w);
  }

  function clearWelcome() {
    const w = log.querySelector('.welcome');
    if (w) w.remove();
  }

  function addMessage(text, who) {
    clearWelcome();
    const row = document.createElement('div');
    row.className = 'row ' + who;
    const avatar = document.createElement('div');
    avatar.className = 'avatar ' + (who === 'user' ? 'me' : 'bot');
    avatar.textContent = who === 'user' ? 'You' : 'A';
    const wrap = document.createElement('div');
    wrap.className = 'bubble-wrap';
    const bubble = document.createElement('div');
    bubble.className = 'bubble';
    bubble.innerHTML = who === 'user' ? '<p>' + escapeHtml(text) + '</p>' : format(text);
    const time = document.createElement('div');
    time.className = 'time';
    time.textContent = now();
    wrap.appendChild(bubble); wrap.appendChild(time);
    row.appendChild(avatar); row.appendChild(wrap);
    log.appendChild(row);
    scrollDown();
    return row;
  }

  function addTyping() {
    clearWelcome();
    const row = document.createElement('div');
    row.className = 'row agent';
    row.innerHTML =
      '<div class="avatar bot">A</div>' +
      '<div class="bubble-wrap"><div class="bubble"><div class="typing">' +
      '<span></span><span></span><span></span></div></div></div>';
    log.appendChild(row);
    scrollDown();
    return row;
  }

  function scrollDown() { log.scrollTop = log.scrollHeight; }

  function resize() {
    input.style.height = 'auto';
    input.style.height = Math.min(input.scrollHeight, 160) + 'px';
    send.disabled = busy || input.value.trim() === '';
  }

  async function submit() {
    const message = input.value.trim();
    if (!message || busy) return;
    busy = true;
    addMessage(message, 'user');
    input.value = ''; resize();
    send.disabled = true;
    const typing = addTyping();
    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message, session_id: sessionId })
      });
      const data = await res.json();
      sessionId = data.session_id;
      typing.remove();
      addMessage(data.reply, 'agent');
    } catch (err) {
      typing.remove();
      addMessage('Sorry, something went wrong reaching the assistant. Please try again.', 'agent');
    } finally {
      busy = false;
      resize();
      input.focus();
    }
  }

  form.addEventListener('submit', e => { e.preventDefault(); submit(); });
  input.addEventListener('input', resize);
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); submit(); }
  });
  document.getElementById('newChat').addEventListener('click', () => {
    sessionId = null; renderWelcome(); input.focus();
  });

  async function checkHealth() {
    try {
      const r = await fetch('/health');
      if (r.ok) { dot.className = 'dot online'; statusText.textContent = 'Online'; return; }
      throw new Error();
    } catch {
      dot.className = 'dot offline'; statusText.textContent = 'Offline';
    }
  }

  renderWelcome();
  resize();
  checkHealth();
  setInterval(checkHealth, 15000);
</script>
</body>
</html>"""


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=settings.web_host, port=settings.web_port)


if __name__ == "__main__":
    main()
