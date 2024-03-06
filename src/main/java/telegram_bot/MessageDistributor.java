package telegram_bot;

import com.rabbitmq.client.*;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Logger;
import static java.nio.charset.StandardCharsets.UTF_8;
@Component
public class MessageDistributor {
    private final TelegramMessageSender messageSender;
    private static final String QUEUE_NAME = "telegram_messages";
    private static final Logger logger = Logger.getLogger(MessageDistributor.class.getName());
    private Connection connection;
    private Channel channel;
    private ScheduledExecutorService executorService;

    @Value("${rabbitmq.host}")
    private String rabbitMqHost;

    @Value("${rabbitmq.username}")
    private String rabbitMqUsername;

    @Value("${rabbitmq.password}")
    private String rabbitMqPassword;

    @Value("${executor.thread-pool-size}")
    private int threadPoolSize;

    public MessageDistributor(TelegramMessageSender messageSender) {
        this.messageSender = messageSender;
    }

    @PostConstruct
    public void initialize() {
        this.executorService = Executors.newScheduledThreadPool(threadPoolSize);
        setupRabbitMQ();
        recoverMessages();
    }

    @EventListener
    public void onMessageEvent(MessageEvent event) {
        queueMessage(event.userId(), event.message());
    }

    private void setupRabbitMQ() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqHost);
        factory.setUsername(rabbitMqUsername);
        factory.setPassword(rabbitMqPassword);
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
                                messageSender.sendTelegramMessage(userId, message, delivery.getEnvelope().getDeliveryTag(), channel),
                        50, TimeUnit.MILLISECONDS);
            }
        };
        try {
            // Выключаем autoAck
            channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {});
        } catch (IOException recoveryError) {
            logger.severe(String.format("Ошибка при восстановлении сообщений из RabbitMQ: %s", recoveryError.getMessage()));
        }
    }
}