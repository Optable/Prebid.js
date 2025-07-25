#!/bin/bash

echo "Starting analytics server on http://localhost:3030"
echo "Events will be logged to: analytics-events.json"
echo ""
echo "To test the setup:"
echo "1. Run this script in one terminal"
echo "2. Open integrationExamples/gpt/optableRtdProvider_example.html in a browser"
echo "3. Watch this terminal for incoming events"
echo "4. Check analytics-events.json for the full event data"
echo ""
echo "Press Ctrl+C to stop the server"
echo "-----------------------------------"

node analytics-server.js