const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = 3030;
const LOG_FILE = path.join(__dirname, 'analytics-events.jsonl');

// Create log file if it doesn't exist
if (!fs.existsSync(LOG_FILE)) {
    fs.writeFileSync(LOG_FILE, '');
}

const server = http.createServer((req, res) => {
    // Enable CORS
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'POST, GET, DELETE, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
    }

    const parsedUrl = url.parse(req.url, true);

    if (req.method === 'POST' && parsedUrl.pathname === '/analytics') {
        let body = '';
        
        req.on('data', chunk => {
            body += chunk.toString();
        });

        req.on('end', () => {
            try {
                const payload = JSON.parse(body);
                
                // Add server timestamp and metadata
                const event = {
                    serverTimestamp: new Date().toISOString(),
                    clientIP: req.connection.remoteAddress,
                    ...payload
                };

                // Append to file as JSONL (one line per event)
                fs.appendFileSync(LOG_FILE, JSON.stringify(event) + '\n');
                
                console.log(`[${event.serverTimestamp}] Received ${payload.eventType || 'unknown'} event`);
                
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: true, message: 'Event logged' }));
            } catch (error) {
                console.error('Error processing request:', error);
                res.writeHead(400, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ success: false, error: error.message }));
            }
        });
    } else if (req.method === 'GET' && parsedUrl.pathname === '/analytics/events') {
        // Read events endpoint for debugging
        try {
            const limit = parseInt(parsedUrl.query.limit) || 100;
            
            if (!fs.existsSync(LOG_FILE)) {
                res.writeHead(200, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify({ total: 0, returned: 0, events: [] }));
                return;
            }
            
            const data = fs.readFileSync(LOG_FILE, 'utf8');
            const lines = data.trim().split('\n').filter(line => line);
            
            // Get last N events
            const events = lines.slice(-limit).map(line => {
                try {
                    return JSON.parse(line);
                } catch (e) {
                    console.error('Error parsing line:', line);
                    return null;
                }
            }).filter(event => event !== null);
            
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({
                total: lines.length,
                returned: events.length,
                events: events
            }));
        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    } else if (req.method === 'DELETE' && parsedUrl.pathname === '/analytics/events') {
        // Clear events endpoint
        try {
            fs.writeFileSync(LOG_FILE, '');
            console.log('Analytics events cleared');
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: true, message: 'Events cleared' }));
        } catch (error) {
            res.writeHead(500, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ success: false, error: error.message }));
        }
    } else if (req.method === 'GET' && parsedUrl.pathname === '/health') {
        // Health check endpoint
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ 
            status: 'ok', 
            timestamp: new Date().toISOString(),
            eventsFile: LOG_FILE
        }));
    } else {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
    }
});

server.listen(PORT, () => {
    console.log(`Analytics server running at http://localhost:${PORT}`);
    console.log(`Events will be logged to: ${LOG_FILE}`);
    console.log('\nEndpoints:');
    console.log('  POST   /analytics        - Log an event');
    console.log('  GET    /analytics/events - Read events (query: ?limit=100)');
    console.log('  DELETE /analytics/events - Clear all events');
    console.log('  GET    /health          - Health check');
});