
const WebSocket = require("ws");
const { createClient } = require("redis");
const { randomUUID } = require("crypto");


const PORT = 3001;
const SERVER_NAME = "Server 2";
const CHANNEL = "polling:events";
const ACTIVE_POLL_KEY = "polling:active";
const CONNECTED_USERS_KEY = "polling:connected_users";

async function startServer() {
    const publisher = createClient({
        url: "redis://127.0.0.1:6379"
    });

    const subscriber = publisher.duplicate();

    publisher.on("error", err =>
        console.error("Redis publisher error:", err.message)
    );

    subscriber.on("error", err =>
        console.error("Redis subscriber error:", err.message)
    );

    await publisher.connect();
    await subscriber.connect();

    async function publishEvent(data) {
        await publisher.publish(CHANNEL, JSON.stringify({
            type: "message",
            server: SERVER_NAME,
            message: JSON.stringify(data),
            timestamp: new Date().toISOString()
        }));
    }

    async function publishConnectedUsers() {
        const storedUsers = await publisher.hGetAll(CONNECTED_USERS_KEY);

        const users = Object.values(storedUsers).map(user =>
            JSON.parse(user)
        );

        await publishEvent({
            type: "users_update",
            users: users
        });
    }

    


    const server = new WebSocket.Server({ port: PORT });

    server.on("listening", () => {
        console.log(`${SERVER_NAME} running on port ${PORT}`);
    });

    server.on("error", err => {
        console.error(`${SERVER_NAME} error:`, err.message);
    });

    server.on("connection", socket => {
        console.log(`Client connected to ${SERVER_NAME}`);

        socket.connectionId = randomUUID();
        socket.username = "";
        socket.role = "";

        socket.send(JSON.stringify({
            type: "welcome",
            message: `Connected to ${SERVER_NAME}`
        }));

        
        socket.on("message", async raw => {
            let data;

            try {
                data = JSON.parse(raw.toString());
            } catch {
                return;
            }

            // Register user and restore active poll after reconnect
            if (data.type === "join") {
                socket.username = data.username;
                socket.role = data.role;

                await publisher.hSet(
                    CONNECTED_USERS_KEY,
                    socket.connectionId,
                    JSON.stringify({
                        connectionId: socket.connectionId,
                        username: socket.username,
                        role: socket.role,
                        server: SERVER_NAME
                    })
                );

                await publishConnectedUsers();

                console.log(`${socket.username} joined ${SERVER_NAME}`);

                if (data.reconnected) {
                    await publishEvent({
                        type: "connection_status",
                        event: "reconnected",
                        username: socket.username,
                        server: SERVER_NAME
                    });
                }

                const savedPoll = await publisher.get(ACTIVE_POLL_KEY);

                if (savedPoll) {
                    socket.send(JSON.stringify({
                        type: "message",
                        server: SERVER_NAME,
                        message: JSON.stringify({
                            type: "create_poll",
                            poll: JSON.parse(savedPoll)
                        }),
                        timestamp: new Date().toISOString()
                    }));

                    console.log(`Active poll restored for ${socket.username}`);
                }

                return;
            }

            // Save the active poll in Redis
            if (data.type === "create_poll" && data.poll) {
                await publisher.set(
                    ACTIVE_POLL_KEY,
                    JSON.stringify(data.poll)
                );
            }

            // Update saved vote counts for future reconnects
            if (data.type === "vote") {
                const savedPoll = await publisher.get(ACTIVE_POLL_KEY);

                if (savedPoll) {
                    const poll = JSON.parse(savedPoll);

                    if (
                        poll.id === data.pollId &&
                        Number.isInteger(data.optionIndex) &&
                        data.optionIndex >= 0 &&
                        data.optionIndex < poll.votes.length
                    ) {
                        poll.votes[data.optionIndex]++;

                        await publisher.set(
                            ACTIVE_POLL_KEY,
                            JSON.stringify(poll)
                        );
                    }
                }
            }

            // Remove saved poll when lecturer closes it
            if (data.type === "close_poll") {
                const savedPoll = await publisher.get(ACTIVE_POLL_KEY);

                if (savedPoll && JSON.parse(savedPoll).id === data.pollId) {
                    await publisher.del(ACTIVE_POLL_KEY);
                }
            }

            await publishEvent(data);
        });


        socket.on("close", async () => {
            console.log(`Client disconnected from ${SERVER_NAME}`);

            if (socket.connectionId) {
                try {
                    await publisher.hDel(
                        CONNECTED_USERS_KEY,
                        socket.connectionId
                    );

                    await publishConnectedUsers();

                } catch (err) {
                    console.error("Could not update connected users:", err.message);
                }
            }

            if (socket.username) {
                try {
                    await publishEvent({
                        type: "connection_status",
                        event: "disconnected",
                        username: socket.username,
                        server: SERVER_NAME
                    });
                } catch (err) {
                    console.error("Could not publish disconnect:", err.message);
                }
            }
        });
    });

    await subscriber.subscribe(CHANNEL, message => {
        for (const socket of server.clients) {
            if (socket.readyState === WebSocket.OPEN) {
                socket.send(message);
            }
        }
    });

    console.log("Redis publisher and subscriber connected");
}

startServer().catch(console.error);
