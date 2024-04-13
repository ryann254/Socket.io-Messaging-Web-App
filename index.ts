import express from 'express';
import { createServer } from 'http';
import { join } from 'path';
import { Server } from 'socket.io';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import { availableParallelism } from 'os';
import cluster from 'cluster';
import { createAdapter, setupPrimary } from '@socket.io/cluster-adapter';

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // create one worker per available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 3000 + i,
    });
  }

  // Setup the adapter on the primary thread
  setupPrimary();
} else {
  // @ts-ignore
  async function main() {
    // Open the database file
    const db = await open({
      filename: 'chat.db',
      driver: sqlite3.Database,
    });
    // Create our `messages` table
    await db.exec(`
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      client_offset TEXT UNIQUE,
      content TEXT
    );
  `);

    const app = express();
    const server = createServer(app);
    const io = new Server(server, {
      connectionStateRecovery: {},
      // setup the adapter on each worker thread
      adapter: createAdapter(),
    });

    app.get('/', (req, res) => {
      res.sendFile(join(__dirname, 'index.html'));
    });

    io.on('connection', async (socket) => {
      socket.on('chat message', async (msg, clientOffset, callback) => {
        let result;
        try {
          // Store the message in the database
          result = await db.run(
            `INSERT INTO messages (content, client_offset) VALUES (?, ?)`,
            msg,
            clientOffset
          );
        } catch (error) {
          if (error.errno === 19 /* SQLITE_CONSTRAINT*/) {
            // The message was already inserted so we notify the client
            callback();
          } else {
            // nothing to do, just let the client retry
          }
          return;
        }
        io.emit('chat message', msg, result.lastID);
        // acknowledge the event
        callback();
      });

      if (!socket.recovered) {
        // If the connection state recovery was not successful
        try {
          await db.each(
            'SELECT id, content FROM messages WHERE id > ?',
            [socket.handshake.auth.serverOffset || 0],
            (_err, row) => {
              {
                socket.emit('chat message', row.content, row.id);
              }
            }
          );
        } catch (error) {
          // Something went wrong
        }
      }
    });

    const port = process.env.PORT;

    server.listen(port, () => {
      console.log(`Server is running on http://localhost:${port}`);
    });
  }
  main();
}
