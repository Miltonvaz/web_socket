const express = require("express");
const socketio = require("socket.io");
const http = require("http");
const amqp = require("amqplib");
require("dotenv").config(); 

const PORT = process.env.PORT || 4004;
const QUEUE = process.env.QUEUE || "notificaciones";
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
const server = http.createServer(app);
const io = socketio(server, {
    cors: {
        origin: "*",
    },
});

async function startRabbitMQ() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE, { durable: true });

        console.log(`Escuchando mensajes en la cola: ${QUEUE}`);

        channel.consume(
            QUEUE,
            (msg) => {
                if (msg !== null) {
                    const messageContent = msg.content.toString();
                    console.log("Nuevo mensaje recibido:", messageContent);

                    io.emit("nuevaCola", `Notificación: ${messageContent}`);
                }
            },
            { noAck: false }
        );
    } catch (error) {
        console.error("Error conectando a RabbitMQ:", error);
    }
}

startRabbitMQ();

io.on("connection", (socket) => {
    console.log("Cliente conectado");

    socket.on("disconnect", () => {
        console.log("Cliente desconectado");
    });
});

server.listen(PORT, () => {
    console.log(`Servidor WebSocket corriendo en el puerto: ${PORT}`);
});
