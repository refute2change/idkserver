// socket-server/server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// Track active hosts using a Map: Key = hostKey, Value = host's socket.id
const activeHosts = new Map();

io.on('connection', (socket) => {
  console.log(`Connected: ${socket.id}`);

  // 1. Handle Role Registration with explicit Host Keys
  socket.on('register-role', ({ role, hostKey }) => {
    if (!hostKey || hostKey.trim() === "") {
      socket.emit('connection-error', 'A valid Room/Host Key is required.');
      return;
    }

    if (role === 'host-server') {
      // Register this socket as the master host for this specific key
      activeHosts.set(hostKey, socket.id);
      socket.join(hostKey); // Host joins their own room
      console.log(`👑 Host registered room: ${hostKey} (${socket.id})`);
      socket.emit('registration-success', { role, hostKey });

    } else if (role === 'regular-client') {
      // Check if the requested host server is currently online
      if (!activeHosts.has(hostKey)) {
        console.log(`🚫 Connection rejected: Host key "${hostKey}" is offline.`);
        socket.emit('connection-error', `Host server "${hostKey}" is not online.`);
        socket.disconnect(); // Force disconnect the client immediately
        return;
      }

      // If online, join the host's room pool
      socket.join(hostKey);
      console.log(`👤 Client ${socket.id} joined host room: ${hostKey}`);
      
      // Notify the specific host that a peer joined
      const targetHostId = activeHosts.get(hostKey);
      io.to(targetHostId).emit('client-joined', { id: socket.id });
      socket.emit('registration-success', { role, hostKey });
    }
  });

  // 2. Relay message from client to their specific room's host
  socket.on('message-to-host', ({ hostKey, payload }) => {
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('client-signal', { senderId: socket.id, payload });
    }
  });

  // 3. Relay message from host to a specific client inside their room
  socket.on('message-from-host', ({ targetClientId, payload }) => {
    io.to(targetClientId).emit('host-signal', payload);
  });

  // Clean up references if a socket drops out
  socket.on('disconnect', () => {
    // Find if this disconnecting socket was an active host
    for (let [key, value] of activeHosts.entries()) {
      if (value === socket.id) {
        activeHosts.delete(key);
        io.to(key).emit('host-disconnected'); // Alert everyone in that specific room
        console.log(`❌ Host room "${key}" went offline.`);
        break;
      }
    }
    console.log(`Disconnected: ${socket.id}`);
  });
});

const PORT = 3000;
server.listen(PORT, () => {
  console.log(`🚀 Multi-Host Server running on http://localhost:${PORT}`);
});