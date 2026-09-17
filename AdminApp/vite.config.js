// vite.config.js
import { defineConfig } from 'vite';
import fs from 'fs';
import path from 'path';

// Change this to the exact path on your disk
const LOG_FILE_PATH = 'C:\\Users\\Tobias\\Desktop\\su2s_siday3_bookingcanceltask\\bookRabbitMq\\logs\\rabbitmq-tracing\\TEST.log'; 
// Windows example: 'C:\\path\\to\\your\\external\\file.log'

export default defineConfig({
  plugins: [
    {
      name: 'external-log-watcher',
      configureServer(server) {
        // Serve initial content via a custom API endpoint
        server.middlewares.use('/api/log', (req, res) => {
          try {
            const content = fs.readFileSync(LOG_FILE_PATH, 'utf-8');
            res.setHeader('Content-Type', 'text/plain');
            res.end(content);
          } catch (err) {
            res.statusCode = 500;
            res.end(`Error reading log file at ${LOG_FILE_PATH}: ${err.message}`);
          }
        });

        // Watch external file for changes on disk
        server.watcher.add(LOG_FILE_PATH);

        server.watcher.on('change', (changedPath) => {
          if (path.resolve(changedPath) === LOG_FILE_PATH) {
            try {
              const content = fs.readFileSync(LOG_FILE_PATH, 'utf-8');
              // Broadcast update to client
              server.ws.send('log-file-updated', { content });
            } catch (err) {
              console.error('Error reading log update:', err);
            }
          }
        });
      },
    },
  ],
});