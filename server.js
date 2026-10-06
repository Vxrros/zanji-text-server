// Zanji Text Server v7
// Backend server for the MicroStudio Zanji mobile game.
//
// Required Render environment variable:
// DATABASE_URL
//
// Optional:
// PORT
// REPLY_FORMAT = raw | envelope | both
//
// NOTE:
// The actual game remains MicroStudio/MicroScript.
// This file is ONLY the online server.

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const postgres = require("postgres");

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("ERROR: DATABASE_URL is missing!");
  process.exit(1);
}

const REPLY_FORMAT = (
  process.env.REPLY_FORMAT || "raw"
).toLowerCase();

const ENVELOPE_NAME =
  process.env.ENVELOPE_NAME || "mp_server_message";

const NUMBER_RE = /^73\d{6}$/;

const sql = postgres(DATABASE_URL, {
  ssl: "require",
  prepare: false,
  max: 5
});

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Zanji number -> Set of connected sockets
const online = new Map();

let nextSocketId = 1;


// ============================================================
// HTTP
// ============================================================

app.get("/", (req, res) => {
  res.send("Zanji Text Server is online!");
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: [...online.keys()]
  });
});


// ============================================================
// DATABASE SETUP
// ============================================================

async function setupDatabase() {
  // Users / Zanji numbers
  await sql`
    CREATE TABLE IF NOT EXISTS zanji_users (
      number TEXT PRIMARY KEY,
      token TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;

  // Messages
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

  // Add this column if the table already existed
  await sql`
    ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS client_msg_id TEXT
  `;

  // Prevent duplicate client message IDs
  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS
    messages_from_client_msg_id_unique
    ON messages (from_number, client_msg_id)
    WHERE client_msg_id IS NOT NULL
  `;

  console.log("Database ready!");
}


// ============================================================
// MICROSTUDIO MESSAGE FORMAT
// ============================================================

function frames(payload) {
  const raw = JSON.stringify(payload);

  const envelope = JSON.stringify({
    name: ENVELOPE_NAME,
    data: payload
  });

  if (REPLY_FORMAT === "both") {
    return [raw, envelope];
  }

  if (REPLY_FORMAT === "envelope") {
    return [envelope];
  }

  // Default: raw
  return [raw];
}


function send(ws, payload) {
  if (!ws) return false;

  if (ws.readyState !== WebSocket.OPEN) {
    console.log(
      `[send] socket#${ws.sid} NOT OPEN state=${ws.readyState}`
    );
    return false;
  }

  const output = frames(payload);

  for (const frame of output) {
    console.log(
      `[send] socket#${ws.sid} -> ${frame}`
    );

    ws.send(frame);
  }

  return true;
}


function sendToNumber(number, payload) {
  const sockets = online.get(number);

  if (!sockets) {
    return 0;
  }

  let sent = 0;

  for (const ws of sockets) {
    if (send(ws, payload)) {
      sent++;
    }
  }

  return sent;
}


// ============================================================
// MICROSTUDIO PROTOCOL
// ============================================================

function unwrap(raw) {

  // MicroStudio client message
  if (
    raw &&
    raw.name === "mp_client_message" &&
    raw.data !== undefined
  ) {
    return {
      kind: "message",
      payload: raw.data
    };
  }

  // MicroStudio connection event
  if (
    raw &&
    raw.name === "mp_client_connection"
  ) {
    return {
      kind: "connection"
    };
  }

  // Other MicroStudio event
  if (
    raw &&
    raw.name
  ) {
    return {
      kind: "other:" + raw.name
    };
  }

  // Direct/raw application message
  if (
    raw &&
    raw.type
  ) {
    return {
      kind: "message",
      payload: raw
    };
  }

  return {
    kind: "unknown"
  };
}


// ============================================================
// ZANJI NUMBERS
// ============================================================

async function newNumber() {

  for (let i = 0; i < 50; i++) {

    const number =
      "73" +
      String(
        Math.floor(
          100000 + Math.random() * 900000
        )
      );

    const rows = await sql`
      SELECT 1
      FROM zanji_users
      WHERE number = ${number}
    `;

    if (rows.length === 0) {
      return number;
    }
  }

  throw new Error("Could not find a free Zanji number");
}


// ============================================================
// REGISTER
// ============================================================

async function handleRegister(ws, payload) {

  let number = String(payload.number || "");
  const token = String(payload.token || "");

  // If the client did not provide a valid number,
  // give it a new one.
  if (!NUMBER_RE.test(number)) {
    number = await newNumber();
  }

  let rows = await sql`
    SELECT token
    FROM zanji_users
    WHERE number = ${number}
  `;

  // New account
  if (rows.length === 0) {

    await sql`
      INSERT INTO zanji_users
        (number, token)
      VALUES
        (${number}, ${token})
      ON CONFLICT DO NOTHING
    `;

    rows = await sql`
      SELECT token
      FROM zanji_users
      WHERE number = ${number}
    `;
  }

  // If the number belongs to another token,
  // issue a new number.
  if (
    rows.length > 0 &&
    rows[0].token !== token
  ) {

    number = await newNumber();

    await sql`
      INSERT INTO zanji_users
        (number, token)
      VALUES
        (${number}, ${token})
    `;

    console.log(
      `[reg] number conflict, issued new number ${number}`
    );
  }

  // Remove this socket from its previous number.
  if (
    ws.zanji &&
    ws.zanji !== number
  ) {
    detach(ws);
  }

  ws.zanji = number;

  if (!online.has(number)) {
    online.set(number, new Set());
  }

  online.get(number).add(ws);

  console.log(
    `[reg] ${number} socket#${ws.sid} ` +
    `online=${online.get(number).size}`
  );

  // Tell the client its number.
  send(ws, {
    type: "assigned",
    number: number
  });

  // Tell the client it is online.
  send(ws, {
    type: "status",
    text: "ONLINE"
  });

  // Deliver saved offline messages.
  await deliverPending(number, ws);
}


// ============================================================
// DELIVER SAVED MESSAGES
// ============================================================

async function deliverPending(number, ws) {

  const rows = await sql`
    SELECT
      id,
      from_number,
      to_number,
      body
    FROM messages
    WHERE
      to_number = ${number}
      AND delivered_at IS NULL
    ORDER BY id ASC
    LIMIT 200
  `;

  for (const message of rows) {

    send(ws, {
      type: "msg",
      id: String(message.id),
      from: message.from_number,
      to: message.to_number,
      body: message.body
    });
  }

  console.log(
    `[pending] ${number}: pushed ${rows.length} message(s)`
  );
}


// ============================================================
// SEND MESSAGE
// ============================================================

async function handleSend(ws, payload) {

  const from = ws.zanji;

  if (!from) {

    send(ws, {
      type: "status",
      text: "Not registered yet."
    });

    return;
  }

  const dest = String(payload.dest || "");

  const body = String(
    payload.body || ""
  ).slice(0, 1000);

  const cid =
    payload.cid
      ? String(payload.cid)
      : null;

  if (!NUMBER_RE.test(dest)) {

    send(ws, {
      type: "status",
      text: "That is not a Zanji number."
    });

    return;
  }

  if (!body) {

    send(ws, {
      type: "status",
      text: "Message was empty."
    });

    return;
  }


  // Save message permanently.
  const inserted = await sql`
    INSERT INTO messages
      (
        from_number,
        to_number,
        body,
        client_msg_id
      )
    VALUES
      (
        ${from},
        ${dest},
        ${body},
        ${cid}
      )
    ON CONFLICT
      (from_number, client_msg_id)
      WHERE client_msg_id IS NOT NULL
    DO NOTHING
    RETURNING id
  `;


  let id;


  // Duplicate message
  if (inserted.length === 0) {

    const existing = await sql`
      SELECT id
      FROM messages
      WHERE
        from_number = ${from}
        AND client_msg_id = ${cid}
    `;

    if (existing.length === 0) {
      throw new Error(
        "Could not find duplicate message"
      );
    }

    id = existing[0].id;

    console.log(
      `[send] duplicate ${from} -> ${dest} cid=${cid}`
    );

    send(ws, {
      type: "sent",
      cid: cid,
      id: String(id),
      online: online.has(dest)
    });

    return;
  }


  id = inserted[0].id;


  // Try to deliver immediately.
  const pushed = sendToNumber(
    dest,
    {
      type: "msg",
      id: String(id),
      from: from,
      to: dest,
      body: body
    }
  );


  console.log(
    `[send] ${from} -> ${dest} ` +
    `id=${id} pushed_to=${pushed} socket(s)`
  );


  // Tell sender whether recipient is currently online.
  send(ws, {
    type: "sent",
    cid: cid,
    id: String(id),
    online: pushed > 0
  });
}


// ============================================================
// ACKNOWLEDGEMENT
// ============================================================

async function handleAck(ws, payload) {

  if (!ws.zanji) {
    return;
  }

  const id = String(
    payload.id || ""
  );

  if (!/^\d+$/.test(id)) {
    return;
  }

  await sql`
    UPDATE messages
    SET delivered_at = NOW()
    WHERE
      id = ${id}
      AND to_number = ${ws.zanji}
      AND delivered_at IS NULL
  `;

  console.log(
    `[ack] ${ws.zanji} id=${id}`
  );
}


// ============================================================
// REMOVE SOCKET
// ============================================================

function detach(ws) {

  if (!ws.zanji) {
    return;
  }

  const sockets =
    online.get(ws.zanji);

  if (!sockets) {
    return;
  }

  sockets.delete(ws);

  if (sockets.size === 0) {
    online.delete(ws.zanji);
  }
}


// ============================================================
// WEBSOCKET CONNECTION
// ============================================================

wss.on("connection", (ws) => {

  ws.sid = nextSocketId++;

  ws.isAlive = true;

  console.log(
    `[conn] socket#${ws.sid} opened`
  );


  // Keep connection alive.
  ws.on("pong", () => {
    ws.isAlive = true;
  });


  // Initial server message.
  send(ws, {
    type: "status",
    text: "Connected to Zanji server"
  });


  // ----------------------------------------------------------
  // Incoming messages
  // ----------------------------------------------------------

  ws.on("message", async (data) => {

    let raw;

    try {

      raw = JSON.parse(
        data.toString()
      );

    } catch (error) {

      console.log(
        `[frame] socket#${ws.sid} received non-JSON data`
      );

      return;
    }


    const unpacked = unwrap(raw);


    console.log(
      `[frame] socket#${ws.sid} ` +
      `${unpacked.kind} ` +
      `${unpacked.payload
        ? unpacked.payload.type || ""
        : ""}`
    );


    // Ignore MicroStudio connection events.
    if (unpacked.kind !== "message") {
      return;
    }


    const payload =
      unpacked.payload || {};


    try {

      if (payload.type === "register") {

        await handleRegister(
          ws,
          payload
        );

      } else if (payload.type === "send") {

        await handleSend(
          ws,
          payload
        );

      } else if (payload.type === "ack") {

        await handleAck(
          ws,
          payload
        );

      } else {

        console.log(
          `[frame] unknown type ${payload.type}`
        );
      }

    } catch (error) {

      console.error(
        "Handler error:",
        error
      );

      send(ws, {
        type: "status",
        text: "Server error."
      });
    }
  });


  // ----------------------------------------------------------
  // Close
  // ----------------------------------------------------------

  ws.on("close", (code, reason) => {

    console.log(
      `[close] socket#${ws.sid} ` +
      `${ws.zanji || "unregistered"} ` +
      `code=${code} ` +
      `reason=${reason ? reason.toString() : ""}`
    );

    detach(ws);
  });


  // ----------------------------------------------------------
  // Error
  // ----------------------------------------------------------

  ws.on("error", (error) => {

    console.error(
      `[error] socket#${ws.sid}`,
      error.message
    );
  });
});


// ============================================================
// WEBSOCKET HEARTBEAT
// ============================================================

setInterval(() => {

  for (const ws of wss.clients) {

    if (ws.isAlive === false) {

      console.log(
        `[heartbeat] terminating socket#${ws.sid}`
      );

      ws.terminate();

      continue;
    }

    ws.isAlive = false;

    try {
      ws.ping();
    } catch (error) {
      console.error(
        "[heartbeat] ping failed:",
        error.message
      );
    }
  }

}, 25000);


// ============================================================
// START SERVER
// ============================================================

(async () => {

  try {

    await setupDatabase();

    const PORT =
      process.env.PORT || 10000;

    server.listen(
      PORT,
      "0.0.0.0",
      () => {

        console.log(
          `Zanji server running on port ${PORT}`
        );

        console.log(
          `REPLY_FORMAT=${REPLY_FORMAT}`
        );
      }
    );

  } catch (error) {

    console.error(
      "Could not start:",
      error
    );

    process.exit(1);
  }

})();
