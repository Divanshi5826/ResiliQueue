require('dotenv').config();
const mongoose = require('mongoose');
const redisClient = require('../config/redis');
const connectDB = require('../config/db');
const Task = require('../models/Task');

const WORKER_ID = process.env.WORKER_ID || `Worker-${Math.floor(Math.random() * 1000)}`;
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES) || 3;
const POLL_INTERVAL = 2000;

async function init() {
    await connectDB();
    await redisClient.connect();
    console.log(`[${WORKER_ID}] Started. Listening for tasks...`);
    pollQueue();
}

async function processTask(taskStr) {
    const task = JSON.parse(taskStr);
    console.log(`[${WORKER_ID}] Processing Task [${task.id}] - Risk: ${task.riskScore}`);

    // Update Mongo Audit Log
    await Task.findOneAndUpdate({ id: task.id }, { status: 'processing', $inc: { retries: 1 } });

    // Simulate complex calculation or external API call
    const delay = Math.floor(Math.random() * 3000) + 2000;
    await new Promise(resolve => setTimeout(resolve, delay));

    // Determine Success/Failure based on Risk Score
    let successRate = 0.85;
    if (task.riskScore > 0.6) successRate = 0.50; // High risk trades fail more often

    const isSuccess = Math.random() < successRate;

    if (isSuccess) {
        console.log(`[${WORKER_ID}] ✅ Task [${task.id}] Completed.`);
        await Task.findOneAndUpdate({ id: task.id }, { status: 'completed', completedAt: new Date() });
        // Cleanup Redis
        await redisClient.hDel('task_details', task.id);
        await redisClient.sRem('processing_locks', task.id);
    } else {
        task.retries += 1;
        console.log(`[${WORKER_ID}] ⚠️ Task [${task.id}] Failed. Attempt ${task.retries}/${MAX_RETRIES}`);

        if (task.retries >= MAX_RETRIES) {
            console.log(`[${WORKER_ID}] ☠️ Task [${task.id}] Moved to Dead Letter Queue.`);
            await Task.findOneAndUpdate({ id: task.id }, {
                status: 'dead_letter',
                errorLog: 'Max retries exceeded evaluating trade constraints'
            });
            // Cleanup from active Redis
            await redisClient.hDel('task_details', task.id);
            await redisClient.sRem('processing_locks', task.id);
        } else {
            console.log(`[${WORKER_ID}] ♻️ Requeuing Task [${task.id}].`);
            await Task.findOneAndUpdate({ id: task.id }, { status: 'failed' });

            // Requeue in Redis
            task.status = 'queued';
            await redisClient.hSet('task_details', task.id, JSON.stringify(task));
            const redisScore = 10000 - task.priority;
            await redisClient.zAdd('task_queue', [{ score: redisScore, value: task.id }]);

            // Remove processing lock
            await redisClient.sRem('processing_locks', task.id);
        }
    }
}

async function pollQueue() {
    try {
        // Redis 3.0 Compatibility: zPopMin doesn't exist, use zRange then zRem
        const topTasks = await redisClient.zRangeWithScores('task_queue', 0, 0);

        if (topTasks && topTasks.length > 0) {
            const taskId = topTasks[0].value;

            // Try to remove it to claim it (Atomic-ish)
            const removed = await redisClient.zRem('task_queue', taskId);

            if (removed > 0) {
                // Place immediately in processing locks to prevent other workers from grabbing it
                await redisClient.sAdd('processing_locks', taskId);

                const taskDataStr = await redisClient.hGet('task_details', taskId);

                if (taskDataStr) {
                    // We await process Task so this worker processes sequentially
                    await processTask(taskDataStr);
                } else {
                    // Cleanup orphaned lock
                    await redisClient.sRem('processing_locks', taskId);
                }
            }
        }
    } catch (error) {
        console.error(`[${WORKER_ID}] Polling Error:`, error);
    } finally {
        setTimeout(pollQueue, POLL_INTERVAL);
    }
}

init();
