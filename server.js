const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const postgres = require("postgres");


// ========================================
// APP / SERVER
// ========================================

const app = express();

const server = http.createServer(app);

const wss = new WebSocket.Server({
  server: server
});


// ========================================
// DATABASE
// ========================================

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


// ========================================
// CONNECTED ZANJI USERS
// ========================================

// Zanji number -> WebSocket
const users = new Map();


// ========================================
// CREATE DATABASE TABLE
// ========================================

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


// ========================================
// HOME PAGE
// ========================================

app.get("/", (req, res) => {

  res.send("Zanji Text Server is online!");

});


// ========================================
// SEND DATA TO CLIENT
// ========================================

function send(socket, data) {

  if (
    socket &&
    socket.readyState === WebSocket.OPEN
  ) {

    socket.send(JSON.stringify(data));

  }

}


// ========================================
// DELIVER SAVED MESSAGES
// ========================================

async function deliverPending(number, socket) {

  try {

    const messages = await sql`
      SELECT
        id,
        from_number,
        to_number,
        body,
        created_at
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


      // Mark this message as delivered

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

  }

  catch (error) {

    console.error(
      "Could not deliver pending messages:",
      error
    );

  }

}


// ========================================
// NEW WEBSOCKET CONNECTION
// ========================================

wss.on("connection", (socket) => {

  console.log("Player connected");


  let number = null;


  // Tell client connection worked

  send(socket, {

    type: "status",

    text: "Connected to Zanji server!"

  });


  // ======================================
  // RECEIVE MESSAGE
  // ======================================

  socket.on("message", async (data) => {

    try {

      const message =
        JSON.parse(data.toString());


      console.log(
        "Message received:",
        message
      );


      // ====================================
      // REGISTER
      // ====================================

      if (message.type === "register") {

        number =
          String(message.number || "");


        if (!number) {

          send(socket, {

            type: "status",

            text:
              "Registration failed: no Zanji number."

          });

          return;

        }


        // If the same number was already
        // connected, close the old connection.

        const oldSocket =
          users.get(number);


        if (
          oldSocket &&
          oldSocket !== socket
        ) {

          try {

            oldSocket.close();

          }

          catch (e) {}

        }


        // Save current connection

        users.set(number, socket);


        // Tell game its number

        send(socket, {

          type: "assigned",

          number: number

        });


        // Tell game it is online

        send(socket, {

          type: "status",

          text: "ONLINE"

        });


        console.log(
          "Registered Zanji number:",
          number
        );


        // =================================
        // DELIVER OLD / OFFLINE MESSAGES
        // =================================

        await deliverPending(
          number,
          socket
        );


        return;

      }


      // ====================================
      // SEND MESSAGE
      // ====================================

      if (message.type === "send") {

        const dest =
          String(message.dest || "");


        const body =
          String(message.body || "");


        // Make sure sender is registered

        if (!number) {

          send(socket, {

            type: "status",

            text: "Not registered yet."

          });

          return;

        }


        // Don't allow empty messages

        if (
          !dest ||
          !body
        ) {

          send(socket, {

            type: "status",

            text: "Message was empty."

          });

          return;

        }


        // =================================
        // SAVE MESSAGE TO DATABASE FIRST
        // =================================

        const saved =
          await sql`
            INSERT INTO messages
              (
                from_number,
                to_number,
                body
              )
            VALUES
              (
                ${number},
                ${dest},
                ${body}
              )
            RETURNING
              id,
              created_at
          `;


        const messageId =
          saved[0].id;


        // =================================
        // CREATE MESSAGE FOR RECIPIENT
        // =================================

        const outgoing = {

          type: "msg",

          from: number,

          to: dest,

          body: body

        };


        // =================================
        // CHECK IF RECIPIENT IS ONLINE
        // =================================

        const recipient =
          users.get(dest);


        if (
          recipient &&
          recipient.readyState === WebSocket.OPEN
        ) {

          // Send ONLY to intended recipient

          send(
            recipient,
            outgoing
          );


          // Mark as delivered

          await sql`
            UPDATE messages
            SET delivered_at = NOW()
            WHERE id = ${messageId}
          `;


          // Tell sender

          send(socket, {

            type: "status",

            text: "DELIVERED"

          });


          console.log(
            number +
            " -> " +
            dest +
            ": delivered"
          );

        }

        else {

          // Recipient is offline.
          // Message stays in database.

          send(socket, {

            type: "status",

            text:
              "SAVED - recipient is offline"

          });


          console.log(
            number +
            " -> " +
            dest +
            ": saved for later"
          );

        }


        return;

      }


      // ====================================
      // UNKNOWN MESSAGE
      // ====================================

      console.log(
        "Unknown message type:",
        message.type
      );

    }


    catch (error) {

      console.error(
        "Message error:",
        error
      );


      send(socket, {

        type: "status",

        text: "Server error."

      });

    }

  });


  // ======================================
  // DISCONNECT
  // ======================================

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


  // ======================================
  // ERROR
  // ======================================

  socket.on("error", (error) => {

    console.error(
      "WebSocket error:",
      error
    );


    if (
      number &&
      users.get(number) === socket
    ) {

      users.delete(number);

    }

  });

});


// ========================================
// START SERVER
// ========================================

async function startServer() {

  try {

    await setupDatabase();


    const PORT =
      process.env.PORT || 10000;


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

  }

  catch (error) {

    console.error(
      "Could not start server:",
      error
    );

    process.exit(1);

  }

}


startServer();
