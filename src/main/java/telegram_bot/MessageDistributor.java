package telegram_bot;

import com.rabbitmq.client.*;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Logger;
import static java.nio.charset.StandardCharsets.UTF_8;

public class MessageDistributor {
    private static final String QUEUE_NAME = "telegram_messages";
    private final TelegramLongPollingBot bot;
    private static final Logger logger = Logger.getLogger(MessageDistributor.class.getName());
    private Connection connection;
    private Channel channel;
    private final ScheduledExecutorService executorService;

    public MessageDistributor(final TelegramLongPollingBot bot) {
        this.bot = bot;
        this.executorService = Executors.newScheduledThreadPool(2);
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
            // Инициализируем "долговечную" очередь
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        } catch (IOException | TimeoutException error) {
            logger.severe(String.format("Ошибка при настройке RabbitMQ: %s", error.getMessage()));
        }
    }

    public void queueMessage(final Long userId, final SendMessage message) {
        try {
            String messageBody = String.format("%d:%s", userId, message.getText());
            // Сообщения постоянные
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .deliveryMode(2)
                    .build();
            channel.basicPublish("", QUEUE_NAME, props, messageBody.getBytes());
        } catch (IOException addError) {
            logger.severe(String.format("Ошибка при добавлении сообщения в очередь RabbitMQ: %s", addError.getMessage()));
        }
    }

    private void recoverMessages() {
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String messageBody = new String(delivery.getBody(), UTF_8);
            String[] parts = messageBody.split(":", 2);
            if (parts.length == 2) {
                Long userId = Long.parseLong(parts[0]);
                String messageText = parts[1];
                SendMessage message = new SendMessage();
                message.setChatId(String.valueOf(userId));
                message.setText(messageText);

                // Планируем отправку сообщения с задержкой в 50 мс
                executorService.schedule(() ->
                                sendTelegramMessage(userId, message, delivery.getEnvelope().getDeliveryTag()),
                        60, TimeUnit.MILLISECONDS);
            }
        };
        try {
            // Выключаем autoAck
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        } catch (IOException recoveryError) {
            logger.severe(String.format("Ошибка при восстановлении сообщений из RabbitMQ: %s", recoveryError.getMessage()));
        }
    }

    private void sendTelegramMessage(final Long userId, final SendMessage message, final long deliveryTag) {
        try {
            bot.executeAsync(message).thenAccept(msg -> {
                try {
                    channel.basicAck(deliveryTag, false); // Подтверждаем после успешной отправки
                } catch (IOException usageError) {
                    logger.severe(String.format("Ошибка при подтверждении сообщения для пользователя: %d, ошибка: %s", userId, usageError.getMessage()));
                }
            }).exceptionally(sendingError -> {
                logger.severe(String.format("Исключение при отправке сообщения пользователю: %d, ошибка: %s", userId, sendingError.getMessage()));
                return null;
            });
        } catch (Exception error) {
            logger.severe(String.format("Исключение при отправке сообщения пользователю: %d, ошибка: %s", userId, error.getMessage()));
        }
    }
}