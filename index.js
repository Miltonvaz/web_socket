const express = require("express");
const socketio = require("socket.io");
const http = require("http");
const amqp = require("amqplib");
require("dotenv").config();

const PORT = process.env.PORT || 4004;
const QUEUE = process.env.QUEUE || "notificaciones";
const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost";

const app = express();
const server = http.createServer(app);
const io = socketio(server, {
  cors: {
    origin: "*",
  },
});

const userSockets = {};

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
          try {
            const messageContent = JSON.parse(msg.content.toString());
            console.log("Mensaje recibido desde RabbitMQ:", messageContent);

            const messageDetails = JSON.parse(messageContent.message);
            console.log("Detalles del mensaje:", messageDetails);

            const { client_id, appointment_id, car_id, test_date, location, status } = messageDetails;

            if (!client_id) {
              console.error("Error: client_id no válido en el mensaje:", messageDetails);
              return channel.ack(msg);
            }

            const notificationMessage = `Tienes una nueva cita agendada para el ${test_date.Time} en ${location}. Estado: ${status}.`;

            if (userSockets[client_id]) {
              for (const sessionId in userSockets[client_id]) {
                userSockets[client_id][sessionId].emit("Notificacion", notificationMessage);
                console.log(`Notificación enviada a ${client_id} (session: ${sessionId}): ${notificationMessage}`);
              }
            } else {
              console.error(`No se encontró el socket_id para el usuario ${client_id}`);
            }

            channel.ack(msg);
          } catch (error) {
            console.error("Error procesando el mensaje de RabbitMQ:", error);
          }
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
  console.log(`Cliente conectado con ID: ${socket.id}`);

  socket.on("registrarUsuario", (user) => {
    const { clientId, sessionId } = user;

    if (!clientId || !sessionId) {
      console.error("Error: clientId o sessionId no válidos:", user);
      return socket.emit("error", "Datos no válidos");
    }

    if (!userSockets[clientId]) {
      userSockets[clientId] = {};
    }

    if (!userSockets[clientId][sessionId]) {
      userSockets[clientId][sessionId] = socket;
      console.log(`Usuario ${clientId} registrado con sessionId: ${sessionId}`);
    } else {
      console.log(`El sessionId ${sessionId} ya está registrado para el usuario ${clientId}`);
    }

    console.log(`Estado actual de userSockets: ${Object.keys(userSockets[clientId]).length} sesiones activas.`);
  });

  socket.on("disconnect", () => {
    console.log(`Cliente desconectado con ID: ${socket.id}`);

    for (const clientId in userSockets) {
      if (userSockets[clientId]) {
        for (const sessionId in userSockets[clientId]) {
          if (userSockets[clientId][sessionId].id === socket.id) {
            delete userSockets[clientId][sessionId];

            if (Object.keys(userSockets[clientId]).length === 0) {
              delete userSockets[clientId];
            }

            console.log(`Socket ${socket.id} eliminado para el usuario ${clientId}`);
            break;
          }
        }
      }
    }
  });
});

server.listen(PORT, () => {
  console.log(`Servidor WebSocket corriendo en el puerto: ${PORT}`);
});
