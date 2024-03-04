package telegram.bot.TelegramBot;

import com.rabbitmq.client.*;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MessageDistributor {
    private final static String QUEUE_NAME = "telegram_messages";
    private final TelegramLongPollingBot bot;
    private static final Logger logger = Logger.getLogger(MessageDistributor.class.getName());
    private Connection connection;
    private Channel channel;
    private final ScheduledExecutorService executorService;

    public MessageDistributor(final TelegramLongPollingBot bot) {
        this.bot = bot;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        setupRabbitMQ();
        recoverMessages();
    }

    private void setupRabbitMQ() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            logger.info("RabbitMQ setup complete");
        } catch (IOException | TimeoutException e) {
            logger.severe("Ошибка при настройке RabbitMQ: " + e.getMessage());
        }
    }

    public void queueMessage(final Long userId, final SendMessage message) {
        try {
            String messageBody = userId + ":" + message.getText();
            channel.basicPublish("", QUEUE_NAME, null, messageBody.getBytes());
            logger.info("Сообщение добавлено в очередь RabbitMQ");

            executorService.scheduleWithFixedDelay(() -> sendTelegramMessage(userId, message), 0, 50, TimeUnit.MILLISECONDS);
        } catch (IOException e) {
            logger.severe("Ошибка при добавлении сообщения в очередь RabbitMQ: " + e.getMessage());
        }
    }


    private void recoverMessages() {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
            String[] parts = messageBody.split(":", 2);
            if (parts.length == 2) {
                Long userId = Long.parseLong(parts[0]);
                String messageText = parts[1];
                SendMessage message = new SendMessage();
                message.setChatId(String.valueOf(userId));
                message.setText(messageText);
                executorService.scheduleWithFixedDelay(() -> sendTelegramMessage(userId, message), 0, 50, TimeUnit.MILLISECONDS);
            }
        };
        try {
            channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {});
        } catch (IOException e) {
            logger.severe("Ошибка при восстановлении сообщений из RabbitMQ: " + e.getMessage());
        }
    }

    private void sendTelegramMessage(Long userId, SendMessage message) {
        try {
            bot.executeAsync(message).thenAccept(msg -> {
                logger.info("Сообщение успешно отправлено пользователю: " + userId);
            }).exceptionally(e -> {
                logger.severe(String.format("Исключение при отправке сообщения пользователю: %s, ошибка: %s", userId, e.getMessage()));
                return null;
            });
        } catch (Exception e) {
            logger.severe(String.format("Исключение при отправке сообщения пользователю: %s, ошибка: %s", userId, e.getMessage()));
        }
    }
}
