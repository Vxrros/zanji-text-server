// =====================================================
// ZANJI TEXT SERVER  (v2)
// MicroStudio external WebSocket server
// Render + Supabase (PostgreSQL)
// =====================================================

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const postgres = require("postgres");

const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL;

// "envelope" -> {name:"mp_server_message", data:{...}}   (default)
// "raw"      -> {...}                                     (plain JSON)
// Switch on Render > Environment > WIRE_MODE if the game shows frames:0
const WIRE_MODE = (process.env.WIRE_MODE || "envelope").toLowerCase();

if (!DATABASE_URL) {
  console.error("ERROR: DATABASE_URL is missing!");
  process.exit(1);
}

const sql = postgres(DATABASE_URL, {
  ssl: "require",
  prepare: false,
  max: 5
});

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// number -> Set of sockets (several sockets per number are allowed,
// so two game instances with the same identity never kick each other)
const online = new Map();
let nextSocketId = 1;

app.get("/", (req, res) => res.send("Zanji Text Server is online!"));
app.get("/health", (req, res) =>
  res.json({ ok: true, wire: WIRE_MODE, numbersOnline: online.size })
);

// -----------------------------------------------------
// Database
// -----------------------------------------------------

async function setupDatabase() {
  await sql`
    CREATE TABLE IF NOT EXISTS zanji_users (
      number TEXT PRIMARY KEY,
      token TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`;

  await sql`
    CREATE TABLE IF NOT EXISTS messages (
      id BIGSERIAL PRIMARY KEY,
      from_number TEXT NOT NULL,
      to_number TEXT NOT NULL,
      body TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      delivered_at TIMESTAMPTZ
    )`;

  await sql`ALTER TABLE messages ADD COLUMN IF NOT EXISTS client_msg_id TEXT`;

  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS messages_from_client_msg_id_unique
    ON messages (from_number, client_msg_id)
    WHERE client_msg_id IS NOT NULL`;

  await sql`
    CREATE INDEX IF NOT EXISTS messages_pending_idx
    ON messages (to_number, id)
    WHERE delivered_at IS NULL`;

  console.log("Database ready!");
}

// -----------------------------------------------------
// Sending
// -----------------------------------------------------

function encode(data) {
  return JSON.stringify(
    WIRE_MODE === "raw" ? data : { name: "mp_server_message", data }
  );
}

function sendTo(ws, data) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(encode(data));
    console.log(`[send] #${ws.sid} ${data.type}`);
    return true;
  } catch (err) {
    console.error(`[send] #${ws.sid} failed:`, err.message);
    return false;
  }
}

function sendToNumber(number, data) {
  const sockets = online.get(number);
  if (!sockets) return 0;
  let n = 0;
  for (const ws of sockets) if (sendTo(ws, data)) n++;
  return n;
}

function addOnline(number, ws) {
  if (!online.has(number)) online.set(number, new Set());
  online.get(number).add(ws);
}

function removeOnline(number, ws) {
  const set = online.get(number);
  if (!set) return;
  set.delete(ws);
  if (set.size === 0) online.delete(number);
}

// -----------------------------------------------------
// Numbers / registration
// -----------------------------------------------------

function validNumber(n) {
  return /^73\d{6}$/.test(n);
}

function randomNumber() {
  return "73" + String(Math.floor(Math.random() * 900000) + 100000);
}

// Returns the number that belongs to this token (the requested one if the
// token matches / the number is free, otherwise a brand new one).
async function claimNumber(requested, token) {
  if (validNumber(requested)) {
    await sql`
      INSERT INTO zanji_users (number, token)
      VALUES (${requested}, ${token})
      ON CONFLICT (number) DO NOTHING`;

    const rows = await sql`
      SELECT token FROM zanji_users WHERE number = ${requested}`;

    if (rows.length && rows[0].token === token) return requested;

    console.log(`[reg] ${requested} belongs to a different token`);
  }

  for (let i = 0; i < 50; i++) {
    const candidate = randomNumber();
    const r = await sql`
      INSERT INTO zanji_users (number, token)
      VALUES (${candidate}, ${token})
      ON CONFLICT (number) DO NOTHING
      RETURNING number`;
    if (r.length) return candidate;
  }
  throw new Error("Could not generate a Zanji number");
}

async function register(ws, data) {
  const token = String(data.token || "").slice(0, 200);
  if (!token) {
    sendTo(ws, { type: "status", text: "Registration failed: no token." });
    return;
  }

  const number = await claimNumber(String(data.number || ""), token);

  if (ws.zanjiNumber && ws.zanjiNumber !== number) {
    removeOnline(ws.zanjiNumber, ws);
  }

  ws.zanjiNumber = number;
  addOnline(number, ws);

  console.log(
    `[reg] ${number} socket#${ws.sid} (${online.get(number).size} socket(s) for this number)`
  );

  sendTo(ws, { type: "assigned", number });
  sendTo(ws, { type: "status", text: "ONLINE" });

  await deliverPending(number, ws);
}

// -----------------------------------------------------
// Pending messages
// A message is only marked delivered when the client ACKs it,
// so a dropped connection can never lose a message.
// The client de-duplicates by id, so re-sending is safe.
// -----------------------------------------------------

async function deliverPending(number, ws) {
  const rows = await sql`
    SELECT id, from_number, to_number, body
    FROM messages
    WHERE to_number = ${number} AND delivered_at IS NULL
    ORDER BY id ASC
    LIMIT 500`;

  console.log(`[pending] ${number}: ${rows.length} message(s)`);

  for (const m of rows) {
    sendTo(ws, {
      type: "msg",
      id: String(m.id),
      from: m.from_number,
      to: m.to_number,
      body: m.body
    });
  }
}

// -----------------------------------------------------
// Send
// -----------------------------------------------------

async function handleSend(ws, data) {
  if (!ws.zanjiNumber) {
    sendTo(ws, { type: "status", text: "Not registered yet." });
    return;
  }

  const from = ws.zanjiNumber;
  const dest = String(data.dest || "");
  const body = String(data.body || "").slice(0, 1000);
  const cid = data.cid ? String(data.cid).slice(0, 100) : null;

  if (!validNumber(dest)) {
    sendTo(ws, { type: "status", text: "That is not a Zanji number." });
    return;
  }
  if (!body) {
    sendTo(ws, { type: "status", text: "Message was empty." });
    return;
  }

  let id;
  let fresh = true;
  let alreadyDelivered = false;

  if (cid) {
    const ins = await sql`
      INSERT INTO messages (from_number, to_number, body, client_msg_id)
      VALUES (${from}, ${dest}, ${body}, ${cid})
      ON CONFLICT (from_number, client_msg_id)
        WHERE client_msg_id IS NOT NULL
      DO NOTHING
      RETURNING id`;

    if (ins.length) {
      id = ins[0].id;
    } else {
      fresh = false;
      const ex = await sql`
        SELECT id, delivered_at FROM messages
        WHERE from_number = ${from} AND client_msg_id = ${cid}`;
      id = ex[0].id;
      alreadyDelivered = ex[0].delivered_at !== null;
    }
  } else {
    const ins = await sql`
      INSERT INTO messages (from_number, to_number, body)
      VALUES (${from}, ${dest}, ${body})
      RETURNING id`;
    id = ins[0].id;
  }

  let isOnline = alreadyDelivered;

  if (fresh) {
    const n = sendToNumber(dest, {
      type: "msg",
      id: String(id),
      from,
      to: dest,
      body
    });
    isOnline = n > 0;
    console.log(
      `[msg] ${from} -> ${dest} #${id} ${isOnline ? "pushed live" : "saved (recipient offline)"}`
    );
  } else {
    console.log(`[msg] duplicate cid ${cid} ignored (#${id})`);
  }

  // Tells the sender it is safe to drop the message from its outbox.
  sendTo(ws, {
    type: "sent",
    cid: cid || "",
    id: String(id),
    online: isOnline
  });
}

async function handleAck(ws, data) {
  if (!ws.zanjiNumber || data.id == null) return;
  const id = String(data.id);
  if (!/^\d{1,18}$/.test(id)) return;

  await sql`
    UPDATE messages
    SET delivered_at = NOW()
    WHERE id = ${id}
      AND to_number = ${ws.zanjiNumber}
      AND delivered_at IS NULL`;
}

async function handleApplicationMessage(ws, data) {
  if (!data || typeof data !== "object") return;

  console.log(`[app] #${ws.sid} type=${data.type}`);

  if (data.type === "register") return register(ws, data);
  if (data.type === "send") return handleSend(ws, data);
  if (data.type === "ack") return handleAck(ws, data);

  console.log(`[app] #${ws.sid} unknown type: ${data.type}`);
}

// -----------------------------------------------------
// WebSocket
// -----------------------------------------------------

wss.on("connection", (ws) => {
  ws.sid = nextSocketId++;
  ws.zanjiNumber = null;
  ws.isAlive = true;

  console.log(`[conn] #${ws.sid} opened`);

  ws.on("pong", () => (ws.isAlive = true));

  ws.on("message", async (rawData) => {
    ws.isAlive = true;
    try {
      const raw = JSON.parse(rawData.toString());

      switch (raw.name) {
        case "mp_client_connection":
          console.log(`[frame] #${ws.sid} mp_client_connection`);
          return;

        case "mp_client_message":
          await handleApplicationMessage(ws, raw.data);
          return;

        case "mp_update":
          return;

        case "mp_client_disconnected":
          // Informational only. The real socket "close" event is what
          // decides whether someone is offline.
          console.log(`[frame] #${ws.sid} mp_client_disconnected (ignored)`);
          return;
      }

      // Tolerate a plain {type:...} frame too
      if (raw && typeof raw.type === "string") {
        await handleApplicationMessage(ws, raw);
        return;
      }

      console.log(`[frame] #${ws.sid} unknown frame:`, raw);
    } catch (err) {
      console.error(`[frame] #${ws.sid} error:`, err);
      sendTo(ws, { type: "status", text: "Server error. Try again." });
    }
  });

  ws.on("close", (code) => {
    console.log(`[close] #${ws.sid} ${ws.zanjiNumber || "unregistered"} code=${code}`);
    if (ws.zanjiNumber) removeOnline(ws.zanjiNumber, ws);
  });

  ws.on("error", (err) => console.error(`[error] #${ws.sid}:`, err.message));
});

// Reap dead sockets + keep Render's proxy from idling the connection
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) {
      console.log(`[heartbeat] terminating dead socket #${ws.sid}`);
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

wss.on("close", () => clearInterval(heartbeat));

// -----------------------------------------------------
// Start
// -----------------------------------------------------

async function startServer() {
  try {
    await setupDatabase();
    server.listen(PORT, "0.0.0.0", () => {
      console.log(`Zanji server running on port ${PORT}`);
      console.log(`Wire mode: ${WIRE_MODE}`);
    });
  } catch (err) {
    console.error("Could not start server:", err);
    process.exit(1);
  }
}

process.on("SIGTERM", async () => {
  console.log("SIGTERM: shutting down");
  try { await sql.end({ timeout: 5 }); } catch (e) {}
  process.exit(0);
});

startServer();
