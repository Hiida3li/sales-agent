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
    app = FastAPI(title="Aura")
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
<title>Aura — AI Shopping Assistant</title>
<style>
  :root {
    --bg: #fbfbfe;
    --surface: #ffffff;
    --surface-2: #f3f4fa;
    --ink: #15172b;
    --ink-soft: #5a6080;
    --ink-faint: #9aa0bd;
    --line: #e7e8f2;
    --brand: #5b4ce0;
    --brand-2: #8b5cf6;
    --brand-strong: #4a3dd0;
    --brand-soft: #efeefe;
    --ok: #22c55e;
    --bad: #ef4444;
    --shadow-sm: 0 1px 2px rgba(20,22,55,.06), 0 6px 18px rgba(20,22,55,.06);
    --shadow-lg: 0 24px 60px rgba(40,30,120,.18);
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #0a0b12; --surface: #12131d; --surface-2: #1a1c29;
      --ink: #eceef7; --ink-soft: #a6abc7; --ink-faint: #6a7090;
      --line: #232636; --brand: #8b80ff; --brand-2: #a78bfa; --brand-strong: #a99dff;
      --brand-soft: #1b1c2e;
      --shadow-sm: 0 1px 2px rgba(0,0,0,.4), 0 8px 24px rgba(0,0,0,.35);
      --shadow-lg: 0 24px 70px rgba(0,0,0,.55);
    }
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; margin: 0; }
  body {
    font-family: "Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    background: var(--bg); color: var(--ink); -webkit-font-smoothing: antialiased;
  }
  .hidden { display: none !important; }
  .brand { display: flex; align-items: center; gap: 11px; }
  .brand .logo {
    width: 40px; height: 40px; border-radius: 12px; display: grid; place-items: center;
    background: linear-gradient(135deg, var(--brand), var(--brand-2));
    color: #fff; font-weight: 800; font-size: 19px; box-shadow: 0 6px 16px rgba(91,76,224,.4);
  }
  .brand .name { font-weight: 700; font-size: 17px; letter-spacing: .2px; }
  .status { display: inline-flex; align-items: center; gap: 7px; font-size: 12.5px; color: var(--ink-soft); }
  .dot { width: 8px; height: 8px; border-radius: 50%; background: var(--ink-faint); }
  .dot.online { background: var(--ok); box-shadow: 0 0 0 3px rgba(34,197,94,.18); }
  .dot.offline { background: var(--bad); box-shadow: 0 0 0 3px rgba(239,68,68,.18); }

  /* ============ LANDING ============ */
  #landing { min-height: 100dvh; display: flex; flex-direction: column; position: relative; overflow: hidden; }
  .blob { position: absolute; border-radius: 50%; filter: blur(70px); opacity: .5; z-index: 0; pointer-events: none; }
  .blob.a { width: 460px; height: 460px; background: #7c6cff; top: -160px; right: -120px; }
  .blob.b { width: 420px; height: 420px; background: #b07bff; bottom: -180px; left: -120px; opacity: .35; }
  .lnav {
    position: relative; z-index: 2; display: flex; align-items: center; justify-content: space-between;
    padding: 22px 32px; max-width: 1120px; margin: 0 auto; width: 100%;
  }
  .hero {
    position: relative; z-index: 2; flex: 1; display: flex; flex-direction: column; align-items: center;
    justify-content: center; text-align: center; padding: 32px 24px 64px; max-width: 860px; margin: 0 auto;
  }
  .pill {
    display: inline-flex; align-items: center; gap: 8px; padding: 7px 14px; border-radius: 999px;
    background: var(--brand-soft); color: var(--brand-strong); font-weight: 600; font-size: 13px;
    border: 1px solid var(--line); margin-bottom: 26px;
  }
  .pill .ping { width: 7px; height: 7px; border-radius: 50%; background: var(--brand); animation: pulse 2s infinite; }
  @keyframes pulse { 0% { box-shadow: 0 0 0 0 rgba(91,76,224,.5);} 70% { box-shadow: 0 0 0 8px rgba(91,76,224,0);} 100%{box-shadow:0 0 0 0 rgba(91,76,224,0);} }
  .hero h1 {
    font-size: clamp(34px, 6vw, 60px); line-height: 1.05; letter-spacing: -1.5px; margin: 0 0 20px; font-weight: 800;
  }
  .hero h1 .grad { background: linear-gradient(120deg, var(--brand), var(--brand-2)); -webkit-background-clip: text;
                   background-clip: text; color: transparent; }
  .hero .sub { font-size: clamp(16px, 2.2vw, 19px); color: var(--ink-soft); max-width: 620px; line-height: 1.6; margin: 0 0 36px; }
  .cta {
    appearance: none; border: 0; cursor: pointer; display: inline-flex; align-items: center; gap: 10px;
    background: linear-gradient(135deg, var(--brand), var(--brand-2)); color: #fff;
    font-size: 16px; font-weight: 650; padding: 16px 30px; border-radius: 14px;
    box-shadow: 0 12px 30px rgba(91,76,224,.4); transition: transform .15s, box-shadow .15s;
  }
  .cta:hover { transform: translateY(-2px); box-shadow: 0 18px 40px rgba(91,76,224,.5); }
  .cta svg { width: 18px; height: 18px; transition: transform .15s; }
  .cta:hover svg { transform: translateX(3px); }
  .features { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-top: 56px; width: 100%; max-width: 820px; }
  .feature {
    background: var(--surface); border: 1px solid var(--line); border-radius: 16px; padding: 22px 20px;
    text-align: left; box-shadow: var(--shadow-sm);
  }
  .feature .ic {
    width: 40px; height: 40px; border-radius: 11px; background: var(--brand-soft); color: var(--brand);
    display: grid; place-items: center; margin-bottom: 14px;
  }
  .feature .ic svg { width: 20px; height: 20px; }
  .feature h3 { margin: 0 0 6px; font-size: 15.5px; }
  .feature p { margin: 0; font-size: 13.5px; color: var(--ink-soft); line-height: 1.5; }
  .lfoot { position: relative; z-index: 2; text-align: center; padding: 22px; color: var(--ink-faint); font-size: 12.5px; }

  /* ============ CHAT ============ */
  #chat { height: 100dvh; display: flex; flex-direction: column; max-width: 880px; margin: 0 auto;
          background: var(--surface); border-left: 1px solid var(--line); border-right: 1px solid var(--line); }
  .chead { display: flex; align-items: center; gap: 14px; padding: 13px 20px; border-bottom: 1px solid var(--line);
           background: var(--surface); }
  .iconbtn { appearance: none; border: 1px solid var(--line); background: transparent; color: var(--ink-soft);
             width: 38px; height: 38px; border-radius: 10px; cursor: pointer; display: grid; place-items: center;
             transition: all .15s; }
  .iconbtn:hover { background: var(--surface-2); color: var(--ink); }
  .iconbtn svg { width: 18px; height: 18px; }
  .chead .who { display: flex; flex-direction: column; line-height: 1.25; }
  .chead .who .name { font-weight: 650; font-size: 15px; }
  .chead .spacer { flex: 1; }

  #log { flex: 1; overflow-y: auto; padding: 24px 20px 10px; display: flex; flex-direction: column; gap: 16px; scroll-behavior: smooth; }
  .row { display: flex; gap: 12px; align-items: flex-end; animation: rise .26s ease both; }
  .row.user { flex-direction: row-reverse; }
  @keyframes rise { from { opacity: 0; transform: translateY(8px);} to { opacity: 1; transform: none; } }
  .av { width: 30px; height: 30px; border-radius: 9px; flex-shrink: 0; display: grid; place-items: center;
        font-size: 12px; font-weight: 700; color: #fff; margin-bottom: 2px; }
  .av.bot { background: linear-gradient(135deg, var(--brand), var(--brand-2)); }
  .av.me { background: var(--surface-2); color: var(--ink-soft); border: 1px solid var(--line); }
  .bw { display: flex; flex-direction: column; max-width: 76%; }
  .row.user .bw { align-items: flex-end; }
  .bubble { padding: 11px 15px; border-radius: 16px; line-height: 1.5; font-size: 14.5px; word-wrap: break-word; box-shadow: var(--shadow-sm); }
  .row.agent .bubble { background: var(--surface); border: 1px solid var(--line); border-bottom-left-radius: 5px; }
  .row.user .bubble { background: linear-gradient(135deg, var(--brand), var(--brand-2)); color: #fff;
                      border-bottom-right-radius: 5px; box-shadow: 0 6px 18px rgba(91,76,224,.3); }
  .bubble p { margin: 0 0 8px; } .bubble p:last-child { margin: 0; }
  .bubble ul { margin: 6px 0; padding-left: 20px; } .bubble li { margin: 2px 0; }
  .bubble code { background: var(--surface-2); padding: 1px 6px; border-radius: 6px; font-size: 13px; font-family: ui-monospace, Menlo, monospace; }
  .time { font-size: 11px; color: var(--ink-faint); margin: 4px 6px 0; }
  .quick { display: flex; flex-wrap: wrap; gap: 8px; margin: 2px 0 2px 42px; }
  .quick button { border: 1px solid var(--line); background: var(--surface); color: var(--ink); padding: 8px 13px;
                  border-radius: 11px; font-size: 13px; cursor: pointer; transition: all .15s; box-shadow: var(--shadow-sm); }
  .quick button:hover { border-color: var(--brand); background: var(--brand-soft); }
  .typing { display: flex; gap: 4px; padding: 13px 15px; }
  .typing span { width: 7px; height: 7px; border-radius: 50%; background: var(--ink-faint); animation: blink 1.3s infinite both; }
  .typing span:nth-child(2){ animation-delay:.18s;} .typing span:nth-child(3){ animation-delay:.36s;}
  @keyframes blink { 0%,80%,100%{opacity:.25; transform: translateY(0);} 40%{opacity:1; transform: translateY(-3px);} }

  .cwrap { padding: 12px 16px 18px; border-top: 1px solid var(--line); background: var(--surface); }
  .composer { display: flex; align-items: flex-end; gap: 10px; background: var(--surface-2); border: 1px solid var(--line);
              border-radius: 16px; padding: 8px 8px 8px 16px; transition: border-color .15s, box-shadow .15s; }
  .composer:focus-within { border-color: var(--brand); box-shadow: 0 0 0 4px var(--brand-soft); }
  textarea { flex: 1; border: 0; background: transparent; resize: none; outline: none; color: var(--ink); font: inherit;
             font-size: 15px; line-height: 1.5; max-height: 150px; padding: 7px 0; }
  textarea::placeholder { color: var(--ink-faint); }
  .send { width: 40px; height: 40px; flex-shrink: 0; border: 0; border-radius: 12px; cursor: pointer;
          background: var(--brand); color: #fff; display: grid; place-items: center; transition: all .15s; }
  .send:hover:not(:disabled) { background: var(--brand-strong); }
  .send:disabled { opacity: .4; cursor: default; }
  .send svg { width: 18px; height: 18px; }
  .hint { text-align: center; font-size: 11.5px; color: var(--ink-faint); margin: 8px 0 0; }
  #log::-webkit-scrollbar { width: 10px; }
  #log::-webkit-scrollbar-thumb { background: var(--line); border-radius: 8px; border: 3px solid var(--surface); }

  @media (max-width: 720px) {
    .features { grid-template-columns: 1fr; }
    #chat { border: 0; } .bw { max-width: 86%; }
  }
</style>
</head>
<body>

  <!-- ===================== LANDING ===================== -->
  <section id="landing">
    <div class="blob a"></div>
    <div class="blob b"></div>

    <nav class="lnav">
      <div class="brand"><div class="logo">A</div><span class="name">Aura</span></div>
      <span class="status"><span id="dotL" class="dot"></span><span id="statusL">connecting…</span></span>
    </nav>

    <div class="hero">
      <span class="pill"><span class="ping"></span> AI Shopping Assistant</span>
      <h1>Shop smarter with <span class="grad">Aura</span></h1>
      <p class="sub">Aura helps you find the right products and answers your questions about
        shipping, returns, payment, and warranty — instantly, any time of day.</p>
      <button id="startBtn" class="cta">
        Start chatting
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"
             stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14M13 6l6 6-6 6"/></svg>
      </button>

      <div class="features">
        <div class="feature">
          <div class="ic"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
            stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="7"/><path d="m21 21-4.3-4.3"/></svg></div>
          <h3>Find products fast</h3>
          <p>Search the catalog by name, color, and price — and get tailored recommendations.</p>
        </div>
        <div class="feature">
          <div class="ic"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
            stroke-linecap="round" stroke-linejoin="round"><path d="M13 2 3 14h7l-1 8 10-12h-7l1-8z"/></svg></div>
          <h3>Instant answers</h3>
          <p>Shipping, returns, payment, and warranty questions resolved in seconds.</p>
        </div>
        <div class="feature">
          <div class="ic"><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
            stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/></svg></div>
          <h3>Always available</h3>
          <p>Round-the-clock help with no queues, no hold music, no waiting.</p>
        </div>
      </div>
    </div>

    <div class="lfoot">Powered by an event-driven agent on Kafka and Google Gemini.</div>
  </section>

  <!-- ===================== CHAT ===================== -->
  <section id="chat" class="hidden">
    <div class="chead">
      <button id="homeBtn" class="iconbtn" title="Back to home" aria-label="Back to home">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
             stroke-linecap="round" stroke-linejoin="round"><path d="M15 18l-6-6 6-6"/></svg>
      </button>
      <div class="brand"><div class="logo">A</div>
        <div class="who"><span class="name">Aura</span>
          <span class="status"><span id="dotC" class="dot"></span><span id="statusC">Online</span></span></div>
      </div>
      <div class="spacer"></div>
      <button id="newBtn" class="iconbtn" title="New conversation" aria-label="New conversation">
        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
             stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 3-6.7L3 8"/><path d="M3 3v5h5"/></svg>
      </button>
    </div>

    <div id="log" aria-live="polite"></div>

    <div class="cwrap">
      <form id="form" class="composer">
        <textarea id="input" rows="1" placeholder="Message Aura…" aria-label="Message"></textarea>
        <button id="send" class="send" type="submit" disabled aria-label="Send">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"
               stroke-linecap="round" stroke-linejoin="round"><path d="M22 2 11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>
        </button>
      </form>
      <p class="hint">Enter to send &middot; Shift + Enter for a new line</p>
    </div>
  </section>

<script>
  const landing = document.getElementById('landing');
  const chat = document.getElementById('chat');
  const log = document.getElementById('log');
  const form = document.getElementById('form');
  const input = document.getElementById('input');
  const send = document.getElementById('send');
  let sessionId = null, busy = false, started = false;

  const QUICK = [
    'Do you have the iPhone 15 Pro in red?',
    'Which phones are in stock?',
    'How long does shipping take?',
    'What is your return policy?',
  ];

  function escapeHtml(s){ return s.replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
  function format(text){
    const lines = escapeHtml(text).split('\n'); let html='', list=false;
    const inline = t => t.replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
                         .replace(/`([^`]+?)`/g,'<code>$1</code>')
                         .replace(/(?<!\*)\*(?!\*)(.+?)\*(?!\*)/g,'<em>$1</em>');
    for (let raw of lines){
      const line = raw.trim();
      if (/^[-*]\s+/.test(line)){ if(!list){html+='<ul>';list=true;} html+='<li>'+inline(line.replace(/^[-*]\s+/,''))+'</li>'; }
      else { if(list){html+='</ul>';list=false;} if(line) html+='<p>'+inline(line)+'</p>'; }
    }
    if (list) html+='</ul>';
    return html || '<p></p>';
  }
  function now(){ return new Date().toLocaleTimeString([], {hour:'2-digit', minute:'2-digit'}); }
  function scrollDown(){ log.scrollTop = log.scrollHeight; }

  function addMessage(text, who){
    const row = document.createElement('div'); row.className = 'row ' + who;
    const av = document.createElement('div'); av.className = 'av ' + (who==='user'?'me':'bot'); av.textContent = who==='user'?'You':'A';
    const bw = document.createElement('div'); bw.className = 'bw';
    const bubble = document.createElement('div'); bubble.className = 'bubble';
    bubble.innerHTML = who==='user' ? '<p>'+escapeHtml(text)+'</p>' : format(text);
    const time = document.createElement('div'); time.className='time'; time.textContent = now();
    bw.appendChild(bubble); bw.appendChild(time); row.appendChild(av); row.appendChild(bw);
    log.appendChild(row); scrollDown(); return row;
  }
  function addQuickReplies(){
    const q = document.createElement('div'); q.className='quick';
    QUICK.forEach(s => { const b=document.createElement('button'); b.type='button'; b.textContent=s;
      b.addEventListener('click', ()=>{ if(!busy){ q.remove(); input.value=s; resize(); submit(); } }); q.appendChild(b); });
    log.appendChild(q); scrollDown();
  }
  function addTyping(){
    const row=document.createElement('div'); row.className='row agent';
    row.innerHTML='<div class="av bot">A</div><div class="bw"><div class="bubble"><div class="typing"><span></span><span></span><span></span></div></div></div>';
    log.appendChild(row); scrollDown(); return row;
  }
  function resize(){ input.style.height='auto'; input.style.height=Math.min(input.scrollHeight,150)+'px'; send.disabled = busy || input.value.trim()===''; }

  async function submit(){
    const message = input.value.trim(); if(!message || busy) return;
    busy = true; addMessage(message,'user'); input.value=''; resize(); send.disabled=true;
    const typing = addTyping();
    try {
      const res = await fetch('/api/chat', { method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ message, session_id: sessionId }) });
      const data = await res.json(); sessionId = data.session_id; typing.remove(); addMessage(data.reply,'agent');
    } catch(err){ typing.remove(); addMessage('Sorry, I had trouble reaching the service. Please try again.','agent'); }
    finally { busy=false; resize(); input.focus(); }
  }

  function openChat(){
    landing.classList.add('hidden'); chat.classList.remove('hidden');
    if (!started){ started = true;
      addMessage("Hi, I'm Aura — your shopping assistant. Ask me about a product, or pick a quick question below to get started.", 'agent');
      addQuickReplies();
    }
    input.focus();
  }
  function goHome(){ chat.classList.add('hidden'); landing.classList.remove('hidden'); }
  function newChat(){ sessionId=null; started=false; log.innerHTML=''; openChat(); }

  document.getElementById('startBtn').addEventListener('click', openChat);
  document.getElementById('homeBtn').addEventListener('click', goHome);
  document.getElementById('newBtn').addEventListener('click', newChat);
  form.addEventListener('submit', e => { e.preventDefault(); submit(); });
  input.addEventListener('input', resize);
  input.addEventListener('keydown', e => { if(e.key==='Enter' && !e.shiftKey){ e.preventDefault(); submit(); } });

  async function checkHealth(){
    const set = (online) => {
      ['L','C'].forEach(suf => {
        const d=document.getElementById('dot'+suf), t=document.getElementById('status'+suf);
        if(d) d.className='dot '+(online?'online':'offline'); if(t) t.textContent = online?'Online':'Offline';
      });
    };
    try { const r=await fetch('/health'); set(r.ok); } catch { set(false); }
  }
  resize(); checkHealth(); setInterval(checkHealth, 15000);
</script>
</body>
</html>"""


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=settings.web_host, port=settings.web_port)


if __name__ == "__main__":
    main()
