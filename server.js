const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const postgres = require("postgres");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const DATABASE_URL = process.env.DATABASE_URL;

if (!DATABASE_URL) {
  console.error("ERROR: DATABASE_URL is missing!");
  process.exit(1);
}

const sql = postgres(DATABASE_URL, {
  ssl: "require",
  prepare: false,
  max: 5
});

const users = new Map();

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

app.get("/", (req, res) => {
  res.send("Zanji Text Server is online!");
});

function send(socket, data) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(data));
  }
}

async function deliverPending(number, socket) {
  try {
    const messages = await sql`
      SELECT id, from_number, to_number, body, created_at
      FROM messages
      WHERE to_number = ${number}
      AND delivered_at IS NULL
      ORDER BY id ASC
    `;

    for (const message of messages) {
      send(socket, {
        type: "msg",
        from: message.from_number,
        to: message.to_number,
        body: message.body
      });

      await sql`
        UPDATE messages
        SET delivered_at = NOW()
        WHERE id = ${message.id}
      `;
    }

    if (messages.length > 0) {
      console.log(
        "Delivered " +
        messages.length +
        " saved message(s) to " +
        number
      );
    }
  } catch (error) {
    console.error("Could not deliver pending messages:", error);
  }
}

wss.on("connection", (socket) => {
  console.log("Player connected");

  let number = null;

  send(socket, {
    type: "status",
    text: "Connected to Zanji server!"
  });

  socket.on("message", async (data) => {
    try {
      // MicroStudio wraps client messages inside:
      // { name: "mp_client_message", data: {...} }

      const raw = JSON.parse(data.toString());

      const message =
        raw &&
        raw.data &&
        typeof raw.data === "object"
          ? raw.data
          : raw;

      console.log("Message received:", raw);
      console.log("Actual payload:", message);

      // -------------------------
      // REGISTER
      // -------------------------

      if (message.type === "register") {
        number = String(message.number || "");

        if (!number) {
          send(socket, {
            type: "status",
            text: "Registration failed: no Zanji number."
          });
          return;
        }

        const oldSocket = users.get(number);

        if (oldSocket && oldSocket !== socket) {
          try {
            oldSocket.close();
          } catch (e) {}
        }

        users.set(number, socket);

        console.log(
          "Registered Zanji number:",
          number
        );

        console.log(
          "Currently online:",
          [...users.keys()]
        );

        send(socket, {
          type: "assigned",
          number: number
        });

        send(socket, {
          type: "status",
          text: "ONLINE"
        });

        await deliverPending(number, socket);

        return;
      }

      // -------------------------
      // SEND MESSAGE
      // -------------------------

      if (message.type === "send") {
        const dest = String(message.dest || "");
        const body = String(message.body || "");

        if (!number) {
          console.log(
            "SEND rejected because sender is not registered."
          );

          send(socket, {
            type: "status",
            text: "Not registered yet."
          });

          return;
        }

        if (!dest || !body) {
          send(socket, {
            type: "status",
            text: "Message was empty."
          });

          return;
        }

        console.log(
          "Sending:",
          number,
          "->",
          dest,
          "|",
          body
        );

        // Save message to database first
        const saved = await sql`
          INSERT INTO messages
            (from_number, to_number, body)
          VALUES
            (${number}, ${dest}, ${body})
          RETURNING id, created_at
        `;

        const messageId = saved[0].id;

        const outgoing = {
          type: "msg",
          from: number,
          to: dest,
          body: body
        };

        const recipient = users.get(dest);

        if (
          recipient &&
          recipient.readyState === WebSocket.OPEN
        ) {
          console.log(
            "Recipient is ONLINE:",
            dest
          );

          send(recipient, outgoing);

          await sql`
            UPDATE messages
            SET delivered_at = NOW()
            WHERE id = ${messageId}
          `;

          send(socket, {
            type: "status",
            text: "DELIVERED"
          });

          console.log(
            number +
            " -> " +
            dest +
            ": DELIVERED"
          );
        } else {
          console.log(
            "Recipient is OFFLINE:",
            dest
          );

          send(socket, {
            type: "status",
            text: "SAVED - recipient is offline"
          });

          console.log(
            number +
            " -> " +
            dest +
            ": SAVED FOR LATER"
          );
        }

        return;
      }

      console.log(
        "Unknown message type:",
        message.type
      );

    } catch (error) {
      console.error("Message error:", error);

      send(socket, {
        type: "status",
        text: "Server error."
      });
    }
  });

  socket.on("close", () => {
    console.log(
      "Player disconnected:",
      number || "unregistered"
    );

    if (
      number &&
      users.get(number) === socket
    ) {
      users.delete(number);
    }
  });

  socket.on("error", (error) => {
    console.error("WebSocket error:", error);

    if (
      number &&
      users.get(number) === socket
    ) {
      users.delete(number);
    }
  });
});

async function startServer() {
  try {
    await setupDatabase();

    const PORT = process.env.PORT || 10000;

    server.listen(
      PORT,
      "0.0.0.0",
      () => {
        console.log(
          "Zanji server running on port " +
          PORT
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
