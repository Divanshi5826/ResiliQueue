const mongoose = require('mongoose');

const TaskSchema = new mongoose.Schema({
    id: { type: String, required: true, unique: true },
    type: { type: String, required: true },
    payload: { type: Object, required: true },
    priority: { type: Number, required: true },
    riskScore: { type: Number, required: true },
    retries: { type: Number, default: 0 },
    status: { type: String, enum: ['queued', 'processing', 'completed', 'failed', 'dead_letter'], default: 'queued' },
    createdAt: { type: Date, default: Date.now },
    completedAt: { type: Date },
    errorLog: { type: String }
});

module.exports = mongoose.model('Task', TaskSchema);
