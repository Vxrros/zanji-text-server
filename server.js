const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

const wss = new WebSocket.Server({
  server: server
});

// Health check
app.get("/", (req, res) => {
  res.send("Zanjii Text Server is online!");
});

// Connected players
const clients = new Set();

wss.on("connection", (socket) => {
  console.log("Player connected");

  clients.add(socket);

  // Tell the player they connected successfully
  socket.send(JSON.stringify({
    type: "system",
    message: "Connected to Zanjii server!"
  }));

  // When a message is received
  socket.on("message", (data) => {
    try {
      const message = JSON.parse(data.toString());

      console.log("Message received:", message);

      // Send the message to everyone connected
      for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
          client.send(JSON.stringify(message));
        }
      }

    } catch (error) {
      console.log("Invalid message:", error);
    }
  });

  // Player disconnected
  socket.on("close", () => {
    console.log("Player disconnected");
    clients.delete(socket);
  });

  // Connection error
  socket.on("error", (error) => {
    console.log("WebSocket error:", error);
    clients.delete(socket);
  });
});

// Render provides the PORT
const PORT = process.env.PORT || 3000;

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Zanjii server running on port ${PORT}`);
});
