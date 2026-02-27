const express = require('express');
const { v4: uuidv4 } = require('uuid');
const redisClient = require('../config/redis');
const Task = require('../models/Task');

const router = express.Router();
const MAX_QUEUE_SIZE = parseInt(process.env.MAX_QUEUE_SIZE) || 10000;

/**
 * Deterministic Rule-Based Routing
 * Calculates priority and risk based on financial trade properties
 */
function analyzeTradePayload(payload) {
    let type = "Standard";
    let priority = 1;
    let riskScore = 0.1;

    // High volume trades get priority and higher risk monitoring
    if (payload.volume && payload.volume > 100000) {
        priority += 5;
        riskScore += 0.4;
        type = "High-Volume";
    }

    if (payload.assetClass === "DERIVATIVE") {
        priority += 2;
        riskScore += 0.3;
        type = "Derivative";
    }

    if (payload.urgent === true) {
        priority += 3;
    }

    return { type, priority, riskScore: Math.min(riskScore, 1.0) };
}

// POST /api/tasks
// Ingests new trades and queues them in Redis
router.post('/', async (req, res) => {
    try {
        // 1. Backpressure Mechanism: Check Queue Depth
        const queueSize = await redisClient.zCard('task_queue');
        if (queueSize >= MAX_QUEUE_SIZE) {
            return res.status(429).json({
                error: "Too Many Requests",
                message: "Broker queue is at maximum capacity. Applying backpressure."
            });
        }

        const payload = req.body;
        const analysis = analyzeTradePayload(payload);

        const taskData = {
            id: uuidv4(),
            type: analysis.type,
            payload: payload,
            priority: analysis.priority,
            riskScore: analysis.riskScore,
            retries: 0,
            status: "queued",
            createdAt: new Date().toISOString()
        };

        // 2. Persist to MongoDB for Audit Logging
        const taskLog = new Task(taskData);
        await taskLog.save();

        // 3. Push to Redis Priority Queue (ZSET)
        // Redis ZSET sorts ascending by score. We want highest priority first, so we invert it or subtract from max.
        // We'll just subtract the priority from 10000 to keep it simple, so Priority 10 -> score 9990. 
        const redisScore = 10000 - taskData.priority;

        // Ensure atomic JSON storage and ZADD 
        await redisClient.hSet('task_details', taskData.id, JSON.stringify(taskData));
        await redisClient.zAdd('task_queue', [{ score: redisScore, value: taskData.id }]);

        res.status(202).json({
            message: "Task accepted by broker",
            taskId: taskData.id,
            queueDepth: queueSize + 1
        });

    } catch (error) {
        console.error("Ingestion Error:", error);
        res.status(500).json({ error: "Internal Broker Error" });
    }
});

// GET /api/status
// Simplified metrics endpoint for the dashboard
router.get('/status', async (req, res) => {
    try {
        // Get top 50 active tasks from Redis
        const activeTaskIds = await redisClient.zRange('task_queue', 0, 49);
        const activeTasks = [];

        if (activeTaskIds.length > 0) {
            const rawTasks = await redisClient.hmGet('task_details', activeTaskIds);
            rawTasks.forEach(t => activeTasks.push(JSON.parse(t)));
        }

        // Processing / Locked tasks
        const lockedTaskIds = await redisClient.sMembers('processing_locks');
        const processingTasks = [];
        if (lockedTaskIds.length > 0) {
            const rawData = await redisClient.hmGet('task_details', lockedTaskIds);
            rawData.forEach(t => processingTasks.push(JSON.parse(t)));
        }

        // Get recent dead letter tasks from Mongo
        const deadLetterTasks = await Task.find({ status: 'dead_letter' })
            .sort({ createdAt: -1 })
            .limit(20)
            .lean();

        // Count metrics from DB
        const totalProcessed = await Task.countDocuments({ status: 'completed' });
        const totalFailed = await Task.countDocuments({ status: { $in: ['failed', 'dead_letter'] } });

        res.json({
            queue: [...processingTasks, ...activeTasks],
            deadLetterQueue: deadLetterTasks,
            metrics: {
                totalProcessed,
                totalFailed,
                queueDepth: activeTaskIds.length + processingTasks.length
            }
        });
    } catch (error) {
        console.error("Status Error:", error);
        res.status(500).json({ error: "Could not fetch status" });
    }
});

module.exports = router;
