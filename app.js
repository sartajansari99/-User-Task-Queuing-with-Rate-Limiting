const express = require('express');
const fs = require('fs');
const rateLimit = require('express-rate-limit');
const Queue = require('bull');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;
const Redis = require('ioredis');

const redisClient = new Redis();

// Task logging function
async function task(user_id) {
  const logMessage = `${user_id} - task completed at - ${new Date().toISOString()}\n`;
  fs.appendFileSync('task_logs.txt', logMessage);
  console.log(`${user_id} - task completed at - ${Date.now()}`);
}

// If master, set up the cluster
if (cluster.isMaster) {
  console.log(`Master ${process.pid} is running`);

  // Fork workers for each CPU
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', (worker, code, signal) => {
    console.log(`Worker ${worker.process.pid} died`);
  });
  
} else {
  // Workers will execute the server code
  const app = express();
  app.use(express.json());

  // Setup task queue using Bull with Redis
  const taskQueue = new Queue('tasks', 'redis://127.0.0.1:6379');

  // Rate limiter for 1 task per second per user
  const taskLimiterSecond = rateLimit({
    windowMs: 1000, // 1 second window
    max: 1, // Limit each user to 1 request per second
    keyGenerator: (req) => req.body.user_id,
  });

  // Rate limiter for 20 tasks per minute per user
  const taskLimiterMinute = rateLimit({
    windowMs: 60 * 1000, // 1 minute window
    max: 20, // Limit each user to 20 requests per minute
    keyGenerator: (req) => req.body.user_id,
  });

  // API Route to handle tasks
  app.post('/api/v1/task', taskLimiterSecond, taskLimiterMinute, async (req, res) => {
    const { user_id } = req.body;

    if (!user_id) {
      return res.status(400).json({ error: 'user_id is required' });
    }

    // Add task to the queue
    await taskQueue.add({ user_id });
    res.status(202).json({ message: 'Task queued' });
  });

  // Processing the task queue with rate limits
  taskQueue.process(async (job) => {
    const { user_id } = job.data;

    // Delay processing according to the rate limit
    const lastTaskTime = await redisClient.get(`user:${user_id}:lastTaskTime`);
    const now = Date.now();
    
    if (lastTaskTime) {
      const delay = Math.max(1000 - (now - lastTaskTime), 0); // Delay for 1 second
      await new Promise(resolve => setTimeout(resolve, delay)); // Wait for the delay
    }

    await task(user_id);
    
    // Update the last task time
    await redisClient.set(`user:${user_id}:lastTaskTime`, now);
  });

  // Handle errors during task processing
  taskQueue.on('failed', (job, err) => {
    console.error(`Job failed for user ${job.data.user_id}: ${err.message}`);
  });

  // Start server
  app.listen(3000, () => {
    console.log(`Worker ${process.pid} started and running on port 3000`);
  });
}
