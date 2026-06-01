// socket-server/server.js
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

const roomPlayerOrders = new Map();

// Track active hosts using a Map: Key = hostKey, Value = host's socket.id
const activeHosts = new Map();

// Track accumulated player points globally: Key = playerId, Value = total points
const playerPointsMap = new Map();

// Track Round4 steal windows per room: Key = hostKey, Value = { attempt: {id,name} | null }
const round4StealState = new Map();

// =================================================================
// UNIFIED SOURCE OF TRUTH LEADERBOARD UTILITY BROADCASTER
// =================================================================
const broadcastRoomLeaderboard = (hostKey) => {
  if (!hostKey) return;
  
  const roomSockets = io.sockets.adapter.rooms.get(hostKey);
  const leaderboard = [];

  if (roomSockets) {
    roomSockets.forEach((socketId) => {
      const targetSocket = io.sockets.sockets.get(socketId);
      // Only include regular game players
      if (targetSocket && targetSocket.data && targetSocket.data.role === 'regular-client') {
        const accumulatedPoints = playerPointsMap.get(socketId) || 0;
        leaderboard.push({
          id: socketId,
          name: targetSocket.data.clientName || `Player ${socketId.slice(0, 6)}`,
          points: accumulatedPoints
        });
      }
    });
  }

  // CRITICAL FIX: REMOVED leaderboard.sort(...) 
  // Players now maintain their static, designated registration order slot!

  // Broadcast the fixed positional array to the ENTIRE room pool instantly
  io.in(hostKey).emit('room-leaderboard-snapshot', { leaderboard });
};


io.on('connection', (socket) => {
  console.log(`Connected: ${socket.id}`);

  // 1. Handle Role Registration with explicit Host Keys
  socket.on('register-role', ({ role, hostKey, clientName }) => {
    if (!hostKey || hostKey.trim() === "") {
      socket.emit('connection-error', 'A valid Room/Host Key is required.');
      return;
    }

    const cleanHostKey = hostKey.trim();

    // --- WORKSPACE 1: HOST CONNECTION INITIALIZATION ---
    if (role === 'host-server') {
      socket.data.role = 'host-server';
      socket.data.hostKey = cleanHostKey;
      activeHosts.set(cleanHostKey, socket.id);
      socket.join(cleanHostKey); 
      console.log(` Host registered room: ${cleanHostKey} (${socket.id})`);
      socket.emit('registration-success', { role, hostKey: cleanHostKey });

      // Synchronize full leaderboard metrics for the host room instantly
      broadcastRoomLeaderboard(cleanHostKey);

    // --- WORKSPACE 2: CLIENT CONNECTION INITIALIZATION ---
    } else if (role === 'regular-client') {
      if (!activeHosts.has(cleanHostKey)) {
        console.log(` Connection rejected: Host key "${cleanHostKey}" is offline.`);
        socket.emit('connection-error', `Host server "${cleanHostKey}" is not online.`);
        socket.disconnect(); 
        return;
      }

      socket.data.role = 'regular-client';
      socket.data.hostKey = cleanHostKey;
      socket.data.clientName = clientName || `Client ${socket.id.slice(0, 6)}`;
      socket.join(cleanHostKey);
      console.log(` Client ${socket.id} joined host room: ${cleanHostKey}`);

      const targetHostId = activeHosts.get(cleanHostKey);
      if (targetHostId) {
        io.to(targetHostId).emit('client-connected', {
          clientId: socket.id,
          clientName: socket.data.clientName,
        });
      }
      
      socket.emit('registration-success', { role, hostKey: cleanHostKey });

      // Synchronize full leaderboard metrics for the client connection instantly
      broadcastRoomLeaderboard(cleanHostKey);
    }
  });

  // 2. Relay message from client to their specific room's host
  socket.on('message-to-host', ({ hostKey, payload }) => {
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('client-signal', {
        senderId: socket.id,
        senderName: socket.data.clientName,
        payload,
      });
    }
  });

  // 3. Relay message from host to a specific client inside their room
  socket.on('message-from-host', ({ targetClientId, payload }) => {
    io.to(targetClientId).emit('host-signal', payload);
  });

  socket.on('select-clue', ({ hostKey, clueIndex, question, final }) => {
    socket.to(hostKey).emit('clue-selected', { clueIndex, question, final });
  });

  // Host reveals the question text to all clients in the room (reveal-only)
  socket.on('reveal-question', ({ hostKey, clueIndex, question, duration, final }) => {
    io.in(hostKey).emit('reveal-question', { clueIndex, question, duration, final });
  });

  // Host starts the answer window; server forwards to clients so they enable typing
  socket.on('start-answer-window', ({ hostKey, clueIndex, duration }) => {
    io.in(hostKey).emit('start-answer-window', { clueIndex, duration });
  });

  socket.on('start-keyword-window', ({ hostKey, duration }) => {
    io.in(hostKey).emit('start-keyword-window', { duration });
  });

  socket.on('close-keyword-window', ({ hostKey }) => {
    io.in(hostKey).emit('close-keyword-window');
  });

  socket.on('clue-state-update', ({ hostKey, clueIndex, opened, answer, final }) => {
    io.in(hostKey).emit('clue-state-update', { clueIndex, opened, answer, final });
  });

  socket.on('clue-answer', ({ hostKey, clueIndex, answer }) => {
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('player-answer', {
        senderId: socket.id,
        senderName: socket.data.clientName,
        clueIndex,
        answer,
      });
    }
  });

  socket.on('keyword-answer', ({ hostKey, answer }) => {
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('keyword-attempt', {
        senderId: socket.id,
        senderName: socket.data.clientName,
        answer,
      });
    }
  });

  socket.on('award-player-points', ({ hostKey, targetClientId, points }) => {
    if (targetClientId && typeof points === 'number') {
      const currentPoints = playerPointsMap.get(targetClientId) || 0;
      const newTotal = Math.max(0, currentPoints + points);
      playerPointsMap.set(targetClientId, newTotal);
      io.to(targetClientId).emit('player-points-update', { points: newTotal });
      io.to(hostKey).emit('player-points-awarded', { playerId: targetClientId, points: newTotal });
      
      // Sync leaderboard
      broadcastRoomLeaderboard(hostKey);
    }
  });

  socket.on('adjust-player-points', ({ hostKey, targetClientId, points, operation }) => {
    if (!targetClientId || typeof points !== 'number') return;
    
    const currentPoints = playerPointsMap.get(targetClientId) || 0;
    
    // 1. Calculate the base raw value depending on the host operational flag
    let newTotal = operation === 'set' ? points : currentPoints + points;
    
    // 2. ABSOLUTE SAFEGUARD: Force a hard mathematical floor clamp at 0 
    // This guarantees points can NEVER be negative, no matter what math was sent!
    newTotal = Math.max(0, newTotal);
    
    // 3. Commit the verified safe score to the cache map
    playerPointsMap.set(targetClientId, newTotal);
    
    // 4. Broadcast synchronized updates out to clients
    io.to(targetClientId).emit('player-points-update', { points: newTotal });
    
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('player-points-awarded', { playerId: targetClientId, points: newTotal });
    }

    // Sync leaderboard elements bar positionally
    broadcastRoomLeaderboard(hostKey);
  });

  socket.on('get-player-points', ({ targetClientId }) => {
    const accumulatedPoints = playerPointsMap.get(targetClientId) || 0;
    io.to(targetClientId).emit('current-player-points', { points: accumulatedPoints });
  });

  socket.on('request-player-points', ({ hostKey, targetClientId }) => {
    if (!targetClientId) return;
    const accumulatedPoints = playerPointsMap.get(targetClientId) || 0;
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('player-points-response', {
        playerId: targetClientId,
        points: accumulatedPoints,
      });
    }
  });

  socket.on('clue-judgement', ({ targetClientId, clueIndex, correct, message }) => {
    io.to(targetClientId).emit('clue-result', { clueIndex, correct, message });
  });

  socket.on('keyword-verdict', ({ hostKey, targetClientId, correct, message }) => {
    if (targetClientId) {
      io.to(targetClientId).emit('keyword-result', { correct, message });
    }
    if (correct) {
      io.in(hostKey).emit('keyword-correct', { correct, message });
      io.in(hostKey).emit('close-keyword-window');
    }
  });

  socket.on('reveal-all-clues', ({ hostKey, clues }) => {
    io.in(hostKey).emit('reveal-all-clues', { clues });
  });

  socket.on('open-round2', ({ hostKey }) => {
    io.in(hostKey).emit('open-round2');
  });

  socket.on('open-round4', ({ hostKey }) => {
    io.in(hostKey).emit('open-round4');
  });

  socket.on('round4-start-question', ({ hostKey, question, value, star, activePlayerId, activePlayerName, duration }) => {
    io.in(hostKey).emit('round4-start-question', { question, value, star, activePlayerId, activePlayerName, duration });
  });

  socket.on('round4-start-timer', ({ hostKey, duration }) => {
    io.in(hostKey).emit('round4-start-timer', { duration });
  });

  socket.on('round4-answer', ({ hostKey, answer }) => {
    const targetHostId = activeHosts.get(hostKey);
    if (targetHostId) {
      io.to(targetHostId).emit('round4-answer', {
        senderId: socket.id,
        senderName: socket.data.clientName,
        answer,
      });
    }
  });

  socket.on('round4-answer-verdict', ({ hostKey, targetClientId, correct, message, points }) => {
    if (!targetClientId) return;
    if (correct && typeof points === 'number') {
      const currentPoints = playerPointsMap.get(targetClientId) || 0;
      const newTotal = Math.max(0, currentPoints + points);
      playerPointsMap.set(targetClientId, newTotal);
      io.to(targetClientId).emit('player-points-update', { points: newTotal });
      io.to(hostKey).emit('player-points-awarded', { playerId: targetClientId, points: newTotal });
    }
    io.to(targetClientId).emit('round4-answer-result', { correct, message, points });

    // Update real-time room leaderboard totals matrix
    broadcastRoomLeaderboard(hostKey);
  });

  socket.on('round4-open-steal-window', ({ hostKey }) => {
    round4StealState.set(hostKey, { attempt: null });
    io.in(hostKey).emit('round4-open-steal-window');
  });

  socket.on('round4-close-steal-window', ({ hostKey }) => {
    round4StealState.delete(hostKey);
    io.in(hostKey).emit('round4-close-steal-window');
  });

  socket.on('round4-steal-attempt', ({ hostKey }) => {
    const state = round4StealState.get(hostKey);
    if (!state || state.attempt) return;
    state.attempt = { id: socket.id, name: socket.data.clientName };
    io.in(hostKey).emit('round4-steal-first', { playerId: socket.id, playerName: socket.data.clientName });
  });

  socket.on('round4-steal-verdict', ({ hostKey, targetClientId, correct, message, points }) => {
    if (!targetClientId) return;
    if (correct && typeof points === 'number') {
      const currentPoints = playerPointsMap.get(targetClientId) || 0;
      const newTotal = Math.max(0, currentPoints + points);
      playerPointsMap.set(targetClientId, newTotal);
      io.to(targetClientId).emit('player-points-update', { points: newTotal });
      io.to(hostKey).emit('player-points-awarded', { playerId: targetClientId, points: newTotal });
    }
    io.to(targetClientId).emit('round4-steal-result', { correct, message, points });

    // Update real-time room leaderboard totals matrix
    broadcastRoomLeaderboard(hostKey);
  });

  socket.on('get-leaderboard', ({ hostKey }) => {
    const leaderboard = Array.from(playerPointsMap.entries())
      .map(([playerId, points]) => ({ playerId, points }))
      .sort((a, b) => b.points - a.points);
    io.in(hostKey).emit('leaderboard-update', { leaderboard });
  });

  socket.on('terminate-game', ({ hostKey }) => {
    io.in(hostKey).emit('terminate-game');
    console.log(`🚪 Host terminated game in room: ${hostKey}`);
  });

  // On-demand snapshot retrieval hook for clients syncing on mount
  socket.on('request-room-leaderboard', ({ hostKey }) => {
    broadcastRoomLeaderboard(hostKey);
  });

  socket.on('kick-client', ({ targetClientId }) => {
    if (!targetClientId) return;
    const targetSocket = io.sockets.sockets.get(targetClientId);
    if (targetSocket) {
      targetSocket.emit('kicked', 'You have been kicked from the host room.');
      targetSocket.disconnect(true);
      console.log(`👢 Kicked client ${targetClientId}`);
    }
  });

  // Clean up references if a socket drops out
socket.on('disconnect', () => {
    const hostKey = socket.data.hostKey;

    if (socket.data.role === 'host-server' && hostKey) {
      activeHosts.delete(hostKey);
      console.log(`❌ Host room "${hostKey}" went offline.`);

      // 1. Send an explicit eviction message to all players still in the room
      io.in(hostKey).emit('host-offline-evict');

      // 2. Fetch all remaining sockets inside this host room channel map
      const roomSockets = io.sockets.adapter.rooms.get(hostKey);
      if (roomSockets) {
        // Create a copy array to prevent loop mutation errors while dropping connections
        const socketIds = Array.from(roomSockets);
        socketIds.forEach((socketId) => {
          const clientSocket = io.sockets.sockets.get(socketId);
          if (clientSocket && clientSocket.id !== socket.id) {
            // Force separate players from server resources completely right here
            clientSocket.disconnect(true);
          }
        });
      }
    } else if (socket.data.role === 'regular-client' && hostKey) {
      const targetHostId = activeHosts.get(hostKey);
      if (targetHostId) {
        io.to(targetHostId).emit('client-disconnected', {
          clientId: socket.id,
        });
      }
      // Re-broadcast leaderboard to track departures
      broadcastRoomLeaderboard(hostKey);
    }

    console.log(`Disconnected: ${socket.id}`);
  });

  // Debug points logger hook
  socket.on('debug-player-points', () => {
    console.log('Current player points:', Array.from(playerPointsMap.entries()));
  });

  socket.on('round4-stage-turn', ({ hostKey, activePlayerId, activePlayerName }) => {
    io.in(hostKey).emit('round4-stage-turn', { activePlayerId, activePlayerName });
  });

  socket.on('round4-turn-over', ({ hostKey }) => {
    // Safe broadcast to wipe player active UI components while preserving room connection parameters
    io.in(hostKey).emit('round4-turn-over');
  });

  socket.on('reorder-players', ({ hostKey, orderedIds }) => {
    // 1. Update the authoritative order for this room
    roomPlayerOrders.set(hostKey, orderedIds);
    
    // 2. Broadcast the new sequence to all clients in the room
    io.in(hostKey).emit('update-player-order', { orderedIds });
    
    console.log(`Ordered list updated for room: ${hostKey}`);
  });
});

const PORT = 3000;
server.listen(PORT, () => {
  console.log(`🚀 Multi-Host Server running on http://localhost:${PORT}`);
});