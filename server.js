// Zanji text server v6 - Render + Supabase
// Env vars: DATABASE_URL (required), PORT (Render sets it),
//           REPLY_FORMAT = envelope | raw | both   (default: envelope)
//           ENVELOPE_NAME = mp_server_message       (default)
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const postgres = require("postgres");

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) { console.error("ERROR: DATABASE_URL is missing"); process.exit(1); }

const REPLY_FORMAT = (process.env.REPLY_FORMAT || "envelope").toLowerCase();
const ENVELOPE_NAME = process.env.ENVELOPE_NAME || "mp_server_message";
const NUMBER_RE = /^73\d{6}$/;

const sql = postgres(DATABASE_URL, { ssl: "require", prepare: false, max: 5 });
const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const online = new Map();   // number -> Set<WebSocket>
let nextSocketId = 1;

app.get("/", (req, res) => res.send("Zanji Text Server is online!"));
app.get("/health", (req, res) => res.json({ ok: true, online: [...online.keys()].length }));

async function setupDatabase() {
  await sql`
    CREATE TABLE IF NOT EXISTS messages (
      id BIGSERIAL PRIMARY KEY,
      from_number TEXT NOT NULL,
      to_number TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      delivered_at TIMESTAMPTZ
    )
  `;
  console.log("Database ready!");
}

// ---------- sending (format is switchable with REPLY_FORMAT) ----------
function frames(payload) {
const raw = JSON.stringify(payload);
const env = JSON.stringify({ name: ENVELOPE_NAME, data: payload });
if (REPLY_FORMAT === "raw") return [raw];
if (REPLY_FORMAT === "both") return [env, raw];
return [env];
}
function send(ws, payload) {
if (!ws || ws.readyState !== WebSocket.OPEN) return false;
for (const f of frames(payload)) ws.send(f);
return true;
}
function sendToNumber(number, payload) {
const set = online.get(number);
let n = 0;
if (set) for (const ws of set) if (send(ws, payload)) n++;
return n;
}

// ---------- protocol unwrap (verified from your Render logs) ----------
function unwrap(raw) {
if (raw && raw.name === "mp_client_message" && raw.data !== undefined) return { kind: "message", payload: raw.data };
if (raw && raw.name === "mp_client_connection") return { kind: "connection" };
if (raw && raw.name) return { kind: "other:" + raw.name };
if (raw && raw.type) return { kind: "message", payload: raw };
return { kind: "unknown" };
}

// ---------- accounts ----------
async function newNumber() {
for (let i = 0; i < 50; i++) {
const n = "73" + String(Math.floor(100000 + Math.random() * 900000));
const rows = await sqlSELECT 1 FROM zanji_users WHERE number = ${n};
if (rows.length === 0) return n;
}
throw new Error("no free number");
}

async function handleRegister(ws, p) {
let number = String(p.number || "");
const token = String(p.token || "");
if (!NUMBER_RE.test(number)) number = await newNumber();
let rows = await sqlSELECT token FROM zanji_users WHERE number = ${number};
if (rows.length === 0) {
await sqlINSERT INTO zanji_users (number, token) VALUES (${number}, ${token}) ON CONFLICT DO NOTHING;
rows = await sqlSELECT token FROM zanji_users WHERE number = ${number};
}
if (rows[0].token !== token) {          // someone else owns this number
number = await newNumber();
await sqlINSERT INTO zanji_users (number, token) VALUES (${number}, ${token});
console.log([reg] number conflict, issued new number ${number});
}
if (ws.zanji && ws.zanji !== number) detach(ws);
ws.zanji = number;
if (!online.has(number)) online.set(number, new Set());
online.get(number).add(ws);          // many sockets per number: nobody gets kicked
console.log([reg] ${number} socket#${ws.sid} online=${online.get(number).size});
send(ws, { type: "assigned", number });
send(ws, { type: "status", text: "ONLINE" });
await deliverPending(number, ws);
}

async function deliverPending(number, ws) {
const rows = await sqlSELECT id, from_number, to_number, body FROM messages   WHERE to_number = ${number} AND delivered_at IS NULL ORDER BY id ASC LIMIT 200;
for (const m of rows) send(ws, { type: "msg", id: String(m.id), from: m.from_number, to: m.to_number, body: m.body });
console.log([pending] ${number}: pushed ${rows.length} (marked delivered only after client ack));
}

async function handleSend(ws, p) {
const from = ws.zanji;
if (!from) return send(ws, { type: "status", text: "Not registered yet." });
const dest = String(p.dest || "");
const body = String(p.body || "").slice(0, 1000);
const cid = p.cid ? String(p.cid) : null;
if (!NUMBER_RE.test(dest)) return send(ws, { type: "status", text: "That is not a Zanji number." });
if (!body) return send(ws, { type: "status", text: "Message was empty." });

let id;
const ins = await sqlINSERT INTO messages (from_number, to_number, body, client_msg_id)   VALUES (${from}, ${dest}, ${body}, ${cid})   ON CONFLICT (from_number, client_msg_id) WHERE client_msg_id IS NOT NULL DO NOTHING   RETURNING id;
if (ins.length === 0) {                 // duplicate resend of the same message: do not insert twice
const ex = await sqlSELECT id FROM messages WHERE from_number = ${from} AND client_msg_id = ${cid};
id = ex[0].id;
console.log([send] duplicate ${from} -> ${dest} cid=${cid});
return send(ws, { type: "sent", cid, id: String(id), online: online.has(dest) });
}
id = ins[0].id;
const n = sendToNumber(dest, { type: "msg", id: String(id), from, to: dest, body });
console.log([send] ${from} -> ${dest} id=${id} pushed_to=${n} socket(s));
send(ws, { type: "sent", cid, id: String(id), online: n > 0 });
}

async function handleAck(ws, p) {
if (!ws.zanji) return;
const id = String(p.id || "");
if (!/^\d+$/.test(id)) return;
await sqlUPDATE messages SET delivered_at = NOW()   WHERE id = ${id} AND to_number = ${ws.zanji} AND delivered_at IS NULL;
console.log([ack] ${ws.zanji} id=${id});
}

function detach(ws) {
const set = ws.zanji && online.get(ws.zanji);
if (set) { set.delete(ws); if (set.size === 0) online.delete(ws.zanji); }
}

wss.on("connection", (ws) => {
ws.sid = nextSocketId++;
ws.isAlive = true;
console.log([conn] socket#${ws.sid} opened);
ws.on("pong", () => { ws.isAlive = true; });
send(ws, { type: "status", text: "Connected to Zanji server" });

ws.on("message", async (data) => {
let raw;
try { raw = JSON.parse(data.toString()); } catch (e) { console.log([frame] socket#${ws.sid} not JSON); return; }
const u = unwrap(raw);
console.log([frame] socket#${ws.sid} ${u.kind} ${u.payload ? u.payload.type : ""});
if (u.kind !== "message") return;
const p = u.payload || {};
try {
if (p.type === "register") await handleRegister(ws, p);
else if (p.type === "send") await handleSend(ws, p);
else if (p.type === "ack") await handleAck(ws, p);
else console.log([frame] unknown type ${p.type});
} catch (err) {
console.error("handler error:", err);
send(ws, { type: "status", text: "Server error." });
}
});
ws.on("close", (code) => { console.log([close] socket#${ws.sid} ${ws.zanji || "unregistered"} code=${code}); detach(ws); });
ws.on("error", (err) => console.error([error] socket#${ws.sid}, err.message));
});

setInterval(() => {                       // keeps Render's proxy from dropping idle sockets
for (const ws of wss.clients) {
if (ws.isAlive === false) { ws.terminate(); continue; }
ws.isAlive = false;
try { ws.ping(); } catch (e) {}
}
}, 25000);

(async () => {
try {
await setupDatabase();
const PORT = process.env.PORT || 10000;
server.listen(PORT, "0.0.0.0", () => console.log(Zanji server on ${PORT}, REPLY_FORMAT=${REPLY_FORMAT}));
} catch (e) { console.error("Could not start:", e); process.exit(1); }
})();
