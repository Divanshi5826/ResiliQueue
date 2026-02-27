require('dotenv').config();
const express = require('express');
const connectDB = require('./config/db');
const redisClient = require('./config/redis');

// Initialize Integrations
connectDB();
redisClient.connect();

const app = express();

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Routes
const taskRoutes = require('./routes/tasks');
app.use('/api/tasks', taskRoutes);

// Compatibility redirect if dashboard expects old endpoint
app.use('/status', (req, res) => res.redirect('/api/tasks/status'));

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`🚀 Broker API running on http://localhost:${PORT}`);
    console.log(`📌 Send POST /api/tasks to ingest payloads`);
});