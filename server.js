const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server: server });

// Zanji number -> WebSocket connection
const users = new Map();

// Messages waiting for users who are temporarily offline
const pending = new Map();


// ------------------------------------
// HOME PAGE / HEALTH CHECK
// ------------------------------------

app.get("/", (req, res) => {
  res.send("Zanji Text Server is online!");
});


// ------------------------------------
// SEND DATA TO A CLIENT
// ------------------------------------

function send(socket, data) {
  if (socket && socket.readyState === WebSocket.OPEN) {
    socket.send(JSON.stringify(data));
  }
}


// ------------------------------------
// SAVE A MESSAGE FOR AN OFFLINE USER
// ------------------------------------

function queueMessage(number, message) {
  if (!pending.has(number)) {
    pending.set(number, []);
  }

  pending.get(number).push(message);
}


// ------------------------------------
// DELIVER SAVED MESSAGES
// ------------------------------------

function deliverPending(number, socket) {
  const messages = pending.get(number);

  if (!messages || messages.length === 0) {
    return;
  }

  for (const message of messages) {
    send(socket, message);
  }

  pending.delete(number);
}


// ------------------------------------
// NEW CONNECTION
// ------------------------------------

wss.on("connection", (socket) => {
  console.log("Player connected");

  let number = null;

  send(socket, {
    type: "status",
    text: "Connected to Zanji server!"
  });


  // ----------------------------------
  // RECEIVE MESSAGE
  // ----------------------------------

  socket.on("message", (data) => {

    try {

      const message = JSON.parse(data.toString());

      console.log("Message received:", message);


      // ==================================
      // REGISTER PLAYER
      // ==================================

      if (message.type === "register") {

        number = String(message.number || "");

        if (!number) {

          send(socket, {
            type: "status",
            text: "Registration failed: no Zanji number."
          });

          return;
        }


        // If this number already has another
        // connection, close the old one.

        const oldSocket = users.get(number);

        if (oldSocket && oldSocket !== socket) {

          try {
            oldSocket.close();
          } catch (e) {}

        }


        // Save this connection
        users.set(number, socket);


        // Tell the game its number
        send(socket, {
          type: "assigned",
          number: number
        });


        // Tell the game it is online
        send(socket, {
          type: "status",
          text: "ONLINE"
        });


        // Send messages that arrived while
        // the player was disconnected.

        deliverPending(number, socket);


        console.log("Registered Zanji number:", number);

        return;
      }


      // ==================================
      // SEND ZANJI MESSAGE
      // ==================================

      if (message.type === "send") {

        const dest = String(message.dest || "");
        const body = String(message.body || "");


        // Player hasn't registered yet
        if (!number) {

          send(socket, {
            type: "status",
            text: "Not registered yet."
          });

          return;
        }


        // Don't allow empty messages
        if (!dest || !body) {

          send(socket, {
            type: "status",
            text: "Message was empty."
          });

          return;
        }


        // This is the format Chapter 4 expects.
        const outgoing = {
          type: "msg",
          from: number,
          to: dest,
          body: body
        };


        // Find the recipient
        const recipient = users.get(dest);


        // ==================================
        // RECIPIENT IS ONLINE
        // ==================================

        if (
          recipient &&
          recipient.readyState === WebSocket.OPEN
        ) {

          // IMPORTANT:
          // Send ONLY to the person who was targeted.

          send(recipient, outgoing);


          // Tell sender the message was delivered
          send(socket, {
            type: "status",
            text: "DELIVERED"
          });


          console.log(
            number + " -> " + dest + ": delivered"
          );

        }


        // ==================================
        // RECIPIENT IS OFFLINE
        // ==================================

        else {

          // Save it temporarily
          queueMessage(dest, outgoing);


          send(socket, {
            type: "status",
            text: "SAVED - recipient is offline"
          });


          console.log(
            number + " -> " + dest + ": queued"
          );

        }

        return;
      }


      // ==================================
      // UNKNOWN MESSAGE
      // ==================================

      console.log(
        "Unknown message type:",
        message.type
      );

    }


    // ====================================
    // INVALID JSON
    // ====================================

    catch (error) {

      console.log(
        "Invalid message:",
        error
      );

      send(socket, {
        type: "status",
        text: "Invalid message."
      });

    }

  });


  // ------------------------------------
  // PLAYER DISCONNECTED
  // ------------------------------------

  socket.on("close", () => {

    console.log(
      "Player disconnected:",
      number || "unregistered"
    );


    // Only remove the connection if this
    // is still the active connection.

    if (
      number &&
      users.get(number) === socket
    ) {

      users.delete(number);

    }

  });


  // ------------------------------------
  // CONNECTION ERROR
  // ------------------------------------

  socket.on("error", (error) => {

    console.log(
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


// ------------------------------------
// START SERVER
// ------------------------------------

// Render provides PORT automatically.
// 0.0.0.0 allows Render to access it.

const PORT = process.env.PORT || 3000;

server.listen(
  PORT,
  "0.0.0.0",
  () => {
    console.log(
      "Zanji server running on port " + PORT
    );
  }
);
