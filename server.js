// =====================================================
// ZANJI TEXT SERVER
// MicroStudio external WebSocket server
// Render + Supabase
// =====================================================

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const postgres = require("postgres");

const PORT = process.env.PORT || 10000;
const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("ERROR: DATABASE_URL is missing!");
  process.exit(1);
}

// -----------------------------------------------------
// Database
// -----------------------------------------------------

const sql = postgres(DATABASE_URL, {
  ssl: "require",
  prepare: false,
  max: 5
});

// -----------------------------------------------------
// HTTP / WebSocket
// -----------------------------------------------------

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
  server
});

// -----------------------------------------------------
// Online users
//
// number -> Set of sockets
// -----------------------------------------------------

const online = new Map();

let nextSocketId = 1;

// -----------------------------------------------------
// Health
// -----------------------------------------------------

app.get("/", (req, res) => {
  res.send("Zanji Text Server is online!");
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    online: [...online.keys()]
  });
});

// -----------------------------------------------------
// Database setup
// -----------------------------------------------------

async function setupDatabase() {
  await sql`
    CREATE TABLE IF NOT EXISTS zanji_users (
      number TEXT PRIMARY KEY,
      token TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;

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

  await sql`
    ALTER TABLE messages
    ADD COLUMN IF NOT EXISTS client_msg_id TEXT
  `;

  await sql`
    CREATE UNIQUE INDEX IF NOT EXISTS
    messages_from_client_msg_id_unique
    ON messages (from_number, client_msg_id)
    WHERE client_msg_id IS NOT NULL
  `;

  console.log("Database ready!");
}

// -----------------------------------------------------
// MicroStudio protocol
//
// MicroStudio exported servers send:
//
// {
//   name: "mp_server_message",
//   data: {...}
// }
//
// Client messages arrive as:
//
// {
//   name: "mp_client_message",
//   data: {...}
// }
// -----------------------------------------------------

function sendMicroStudio(ws, data) {
  if (!ws) return false;

  if (ws.readyState !== WebSocket.OPEN) {
    console.log(
      `[send] socket#${ws.sid} not open (${ws.readyState})`
    );
    return false;
  }

  const packet = {
    name: "mp_server_message",
    data: data
  };

  const text = JSON.stringify(packet);

  console.log(`[send] socket#${ws.sid} -> ${text}`);

  try {
    ws.send(text);
    return true;
  } catch (err) {
    console.error(
      `[send] socket#${ws.sid} failed:`,
      err
    );
    return false;
  }
}

// -----------------------------------------------------
// Online helpers
// -----------------------------------------------------

function addOnline(number, ws) {
  if (!online.has(number)) {
    online.set(number, new Set());
  }

  online.get(number).add(ws);
}

function removeOnline(number, ws) {
  if (!number) return;

  const sockets = online.get(number);

  if (!sockets) return;

  sockets.delete(ws);

  if (sockets.size === 0) {
    online.delete(number);
  }
}

function sendToNumber(number, data) {
  const sockets = online.get(number);

  if (!sockets) return 0;

  let sent = 0;

  for (const ws of sockets) {
    if (sendMicroStudio(ws, data)) {
      sent++;
    }
  }

  return sent;
}

// -----------------------------------------------------
// Zanji numbers
// -----------------------------------------------------

function validNumber(number) {
  return /^73\d{6}$/.test(number);
}

async function generateNumber() {
  for (let attempt = 0; attempt < 100; attempt++) {
    const number =
      "73" +
      String(
        Math.floor(Math.random() * 900000) + 100000
      );

    const existing = await sql`
      SELECT number
      FROM zanji_users
      WHERE number = ${number}
      LIMIT 1
    `;

    if (existing.length === 0) {
      return number;
    }
  }

  throw new Error("Could not generate a Zanji number.");
}

// -----------------------------------------------------
// Register
// -----------------------------------------------------

async function register(ws, data) {
  let requestedNumber = String(data.number || "");
  let token = String(data.token || "");

  if (!token) {
    token =
      "t" +
      Math.floor(Math.random() * 1000000000) +
      "x" +
      Math.floor(Math.random() * 1000000000);
  }

  let number = requestedNumber;

  // New / invalid number
  if (!validNumber(number)) {
    number = await generateNumber();
  }

  // Check existing owner
  const existing = await sql`
    SELECT number, token
    FROM zanji_users
    WHERE number = ${number}
    LIMIT 1
  `;

  if (existing.length > 0) {
    // Correct token = returning owner
    if (existing[0].token !== token) {
      console.log(
        `[reg] ${number} belongs to another token`
      );

      number = await generateNumber();
    }
  } else {
    await sql`
      INSERT INTO zanji_users (number, token)
      VALUES (${number}, ${token})
    `;
  }

  // Save token if this number was newly assigned
  await sql`
    INSERT INTO zanji_users (number, token)
    VALUES (${number}, ${token})
    ON CONFLICT (number)
    DO UPDATE SET token = zanji_users.token
  `;

  // ---------------------------------------------------
  // Replace stale sockets for this number
  // ---------------------------------------------------

  const oldSockets = online.get(number);

  if (oldSockets) {
    for (const old of oldSockets) {
      if (old !== ws) {
        console.log(
          `[reg] closing stale socket#${old.sid} for ${number}`
        );

        old.zanjiNumber = null;

        try {
          old.close(4001, "Replaced by newer connection");
        } catch (err) {}
      }
    }

    online.delete(number);
  }

  ws.zanjiNumber = number;
  ws.zanjiToken = token;

  addOnline(number, ws);

  console.log(
    `[reg] ${number} socket#${ws.sid} online`
  );

  console.log(
    `[reg] online numbers:`,
    [...online.keys()]
  );

  // ---------------------------------------------------
  // IMPORTANT:
  // Send ONLY through the MicroStudio protocol.
  // ---------------------------------------------------

  sendMicroStudio(ws, {
    type: "assigned",
    number: number
  });

  sendMicroStudio(ws, {
    type: "status",
    text: "ONLINE"
  });

  await deliverPending(number, ws);
}

// -----------------------------------------------------
// Pending messages
// -----------------------------------------------------

async function deliverPending(number, ws) {
  const messages = await sql`
    SELECT
      id,
      from_number,
      to_number,
      body
    FROM messages
    WHERE to_number = ${number}
      AND delivered_at IS NULL
    ORDER BY id ASC
  `;

  console.log(
    `[pending] ${number}: ${messages.length} message(s)`
  );

  for (const message of messages) {
    const delivered = sendMicroStudio(ws, {
      type: "msg",
      id: String(message.id),
      from: message.from_number,
      to: message.to_number,
      body: message.body
    });

    if (delivered) {
      await sql`
        UPDATE messages
        SET delivered_at = NOW()
        WHERE id = ${message.id}
          AND delivered_at IS NULL
      `;

      console.log(
        `[pending] delivered #${message.id} -> ${number}`
      );
    }
  }
}

// -----------------------------------------------------
// Send message
// -----------------------------------------------------

async function handleSend(ws, data) {
  if (!ws.zanjiNumber) {
    sendMicroStudio(ws, {
      type: "status",
      text: "Not registered yet."
    });

    return;
  }

  const dest = String(data.dest || "");
  const body = String(data.body || "");

  if (!validNumber(dest)) {
    sendMicroStudio(ws, {
      type: "status",
      text: "That is not a Zanji number."
    });

    return;
  }

  if (!body) {
    sendMicroStudio(ws, {
      type: "status",
      text: "Message was empty."
    });

    return;
  }

  // ---------------------------------------------------
  // Save first.
  //
  // This guarantees offline messages survive
  // Render restarts.
  // ---------------------------------------------------

  const inserted = await sql`
    INSERT INTO messages
      (from_number, to_number, body)
    VALUES
      (${ws.zanjiNumber}, ${dest}, ${body})
    RETURNING id
  `;

  const id = inserted[0].id;

  console.log(
    `[msg] ${ws.zanjiNumber} -> ${dest} #${id}`
  );

  // ---------------------------------------------------
  // Try immediate delivery
  // ---------------------------------------------------

  const sent = sendToNumber(dest, {
    type: "msg",
    id: String(id),
    from: ws.zanjiNumber,
    to: dest,
    body: body
  });

  if (sent > 0) {
    await sql`
      UPDATE messages
      SET delivered_at = NOW()
      WHERE id = ${id}
        AND delivered_at IS NULL
    `;

    console.log(
      `[msg] #${id} delivered immediately`
    );

    sendMicroStudio(ws, {
      type: "status",
      text: "DELIVERED"
    });
  } else {
    console.log(
      `[msg] #${id} saved for offline recipient ${dest}`
    );

    sendMicroStudio(ws, {
      type: "status",
      text: "SAVED - recipient is offline"
    });
  }
}

// -----------------------------------------------------
// MicroStudio application message
// -----------------------------------------------------

async function handleApplicationMessage(ws, data) {
  if (!data || typeof data !== "object") {
    return;
  }

  console.log(
    `[app] socket#${ws.sid}`,
    data
  );

  if (data.type === "register") {
    await register(ws, data);
    return;
  }

  if (data.type === "send") {
    await handleSend(ws, data);
    return;
  }

  if (data.type === "ack") {
    if (data.id != null) {
      await sql`
        UPDATE messages
        SET delivered_at = NOW()
        WHERE id = ${data.id}
          AND delivered_at IS NULL
      `;
    }

    return;
  }

  console.log(
    `[app] unknown type from socket#${ws.sid}:`,
    data.type
  );
}

// -----------------------------------------------------
// WebSocket
// -----------------------------------------------------

wss.on("connection", (ws) => {
  ws.sid = nextSocketId++;
  ws.zanjiNumber = null;
  ws.zanjiToken = null;

  console.log(
    `[conn] socket#${ws.sid} opened`
  );

  ws.on("message", async (rawData) => {
    try {
      const raw = JSON.parse(rawData.toString());

      console.log(
        `[frame] socket#${ws.sid}`,
        raw.name || "unknown"
      );

      // -------------------------------------------------
      // MicroStudio -> server connection notification
      // -------------------------------------------------

      if (raw.name === "mp_client_connection") {
        console.log(
          `[frame] socket#${ws.sid} mp_client_connection`
        );

        return;
      }

      // -------------------------------------------------
      // MicroStudio application message
      // -------------------------------------------------

      if (raw.name === "mp_client_message") {
        console.log(
          `[frame] socket#${ws.sid} mp_client_message`
        );

        await handleApplicationMessage(
          ws,
          raw.data
        );

        return;
      }

      // -------------------------------------------------
      // MicroStudio update tick
      //
      // The official exported MicroStudio server uses
      // this to advance serverUpdate().
      //
      // Our Node server handles application messages
      // immediately, so no action is required here.
      // -------------------------------------------------

      if (raw.name === "mp_update") {
        return;
      }

      // -------------------------------------------------
      // MicroStudio disconnected notification
      // -------------------------------------------------

      if (raw.name === "mp_client_disconnected") {
        console.log(
          `[frame] socket#${ws.sid} mp_client_disconnected`
        );

        if (ws.zanjiNumber) {
          removeOnline(
            ws.zanjiNumber,
            ws
          );
        }

        ws.zanjiNumber = null;

        return;
      }

      console.log(
        `[frame] socket#${ws.sid} unknown frame`,
        raw
      );

    } catch (error) {
      console.error(
        `[frame] socket#${ws.sid} error:`,
        error
      );
    }
  });

  ws.on("close", (code, reason) => {
    const number = ws.zanjiNumber;

    console.log(
      `[close] socket#${ws.sid} ${number || "unregistered"} code=${code} reason=${reason || ""}`
    );

    // IMPORTANT:
    // Only remove THIS socket.
    //
    // If a newer socket replaced this one,
    // it won't accidentally delete the new connection.
    if (number) {
      removeOnline(number, ws);
    }
  });

  ws.on("error", (error) => {
    console.error(
      `[error] socket#${ws.sid}:`,
      error
    );
  });
});

// -----------------------------------------------------
// Start
// -----------------------------------------------------

async function startServer() {
  try {
    await setupDatabase();

    server.listen(
      PORT,
      "0.0.0.0",
      () => {
        console.log(
          `Zanji server running on port ${PORT}`
        );

        console.log(
          "MicroStudio protocol: ENABLED"
        );

        console.log(
          "Supabase persistence: ENABLED"
        );
      }
    );

  } catch (error) {
    console.error(
      "Could not start server:",
      error
    );

    process.exit(1);
  }
}

startServer();
