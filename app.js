const express = require('express');
const mysql = require('mysql2/promise');
const mqtt = require('mqtt');

const app = express();


const mqttBroker = 'mqtt://localhost';
const mqttClient = mqtt.connect(mqttBroker);

const dbConfig = {
    host: '34.48.97.254',
    user: 'root',   
    password: 'Root',  
    database: 'MyDatabase' 
};


app.use(express.json());

async function createConnection() {
    return await mysql.createConnection(dbConfig);
}


mqttClient.on('connect', () => {
    console.log('Connected to MQTT broker');
    mqttClient.subscribe('new_post');
});

// Create a new user
app.post('/api/users', async (req, res) => {
    const { username, email } = req.body;
    const connection = await createConnection();
    try {
        const [result] = await connection.execute('INSERT INTO Users (username, email) VALUES (?, ?)', [username, email]);
        const insertedUserId = result.insertId;
        res.status(201).json({ id: insertedUserId, username, email });
    } catch (error) {
        console.error('Error creating user:', error);
        res.status(500).json({ error: 'Error creating user' });
    } finally {
        await connection.end();
    }
});

// Get all users
app.get('/api/users', async (req, res) => {
    const connection = await createConnection();
    try {
        const [rows] = await connection.execute('SELECT * FROM Users');
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error getting users:', error);
        res.status(500).json({ error: 'Error getting users' });
    } finally {
        await connection.end();
    }
});

// Get a single user by ID
app.get('/api/users/:id', async (req, res) => {
    const userId = req.params.id;
    const connection = await createConnection();
    try {
        const [rows] = await connection.execute('SELECT * FROM Users WHERE user_id = ?', [userId]);
        if (rows.length === 0) {
            res.status(404).json({ error: 'User not found' });
        } else {
            res.status(200).json(rows[0]);
        }
    } catch (error) {
        console.error('Error getting user:', error);
        res.status(500).json({ error: 'Error getting user' });
    } finally {
        await connection.end();
    }
});

// Update a user by ID
app.put('/api/users/:id', async (req, res) => {
    const userId = req.params.id;
    const { username, email, password } = req.body;
    const connection = await createConnection();
    try {
        await connection.execute('UPDATE Users SET username = ?, email = ?, password= ? WHERE user_id = ?', [username, email, password, userId]);
        res.status(200).json({ id: userId, username, email, password });
    } catch (error) {
        console.error('Error updating user:', error);
        res.status(500).json({ error: 'Error updating user' });
    } finally {
        await connection.end();
    }
});

//Create a post per user
app.post('/api/users/:userId/posts', async (req, res) => {
    const userId = req.params.userId;
    const { title, content } = req.body;
    const connection = await createConnection();
    try {
        const [result] = await connection.execute('INSERT INTO Posts (user_id, title, content) VALUES (?, ?, ?)', [userId, title, content]);
        const insertedPostId = result.insertId;
        
        mqttClient.publish('new_post', JSON.stringify({ userId, postId: insertedPostId, title, content }));
        
        res.status(201).json({ id: insertedPostId, user_id: userId, title, content });
    } catch (error) {
        console.error('Error creating post:', error);
        res.status(500).json({ error: 'Error creating post' });
    } finally {
        await connection.end();
    }
});

mqttClient.on('message', async (topic, message) => {
    console.log(`Received message on topic ${topic}: ${message}`);
    
    if (topic === 'new_post') {
        const data = JSON.parse(message);
        const { userId, postId, title, content } = data;
        const connection = await createConnection();
        try {
            // Update user's post count in the database
            await connection.execute('UPDATE Users SET post_count = post_count + 1 WHERE user_id = ?', [userId]);
            console.log(`User ${userId} post count updated in the database`);
        } catch (error) {
            console.error('Error updating user post count:', error);
        } finally {
            await connection.end();
        }
    }
});

//Get all posts by user
app.get('/api/users/:userId/posts', async (req, res) => {
    const userId = req.params.userId;
    const connection = await createConnection();
    try {
        const [rows] = await connection.execute('SELECT * FROM Posts WHERE user_id = ?', [userId]);
        res.status(200).json(rows);
    } catch (error) {
        console.error('Error getting posts:', error);
        res.status(500).json({ error: 'Error getting posts' });
    } finally {
        await connection.end();
    }
});

async function deleteAllPostsByUser(userId) {
    const connection = await createConnection();
    try {
        // Delete all posts by the user from the Posts table
        await connection.execute('DELETE FROM Posts WHERE user_id = ?', [userId]);
        console.log('All posts by the user deleted successfully');
    } catch (error) {
        console.error('Error deleting posts by user:', error);
    } finally {
        await connection.end();
    }
}

//Delete all posts by user
app.delete('/api/users/:userId/posts', async (req, res) => {
    const userId = req.params.userId;
    await deleteAllPostsByUser(userId);
    res.status(204).send();
});


// Start the Express server
const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});
