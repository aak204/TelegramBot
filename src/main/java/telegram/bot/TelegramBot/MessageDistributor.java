package telegram.bot.TelegramBot;

import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MessageDistributor {
    private final Map<Long, BlockingQueue<SendMessage>> userMessageQueues = new ConcurrentHashMap<>();
    private final Map<Long, ScheduledExecutorService> userSchedulers = new ConcurrentHashMap<>();
    private final TelegramLongPollingBot bot;
    private static final Logger logger = Logger.getLogger(MessageDistributor.class.getName());
    private final String dbUrl = "jdbc:postgresql://localhost:5432/telegrambotdb";
    private final String user = "telegrambot";
    private final String password = "bot";

    public MessageDistributor(final TelegramLongPollingBot bot) {
        this.bot = bot;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            logger.severe("PostgreSQL JDBC Driver не найден. Включите его в ваш library path ");
        }
        recoverMessages();
    }

    public void queueMessage(final Long userId, final SendMessage message) {
        // Добавляем сообщение в БД перед отправкой
        if (addToDatabase(userId, message.getText())) {
            userMessageQueues.computeIfAbsent(userId, k -> {
                BlockingQueue<SendMessage> queue = new LinkedBlockingQueue<>();
                ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
                scheduler.scheduleWithFixedDelay(() -> processQueue(userId, queue), 0, 50, TimeUnit.MILLISECONDS);
                userSchedulers.put(userId, scheduler);
                return queue;
            }).offer(message);
        }
    }

    private void processQueue(Long userId, BlockingQueue<SendMessage> queue) {
        SendMessage msg = queue.poll();
        if (msg != null) {
            try {
                bot.executeAsync(msg);
                markMessageAsSent(userId, msg.getText());
            } catch (Exception e) {
                logger.severe(String.format("Исключение при отправке сообщения пользователю: %s, ошибка: %s", userId, e.getMessage()));
            }
        }
    }

    private boolean addToDatabase(Long userId, String messageText) {
        try (Connection connection = DriverManager.getConnection(dbUrl, user, password);
             PreparedStatement stmt = connection.prepareStatement("INSERT INTO messages (user_id, message, sent) VALUES (?, ?, false)")) {
            stmt.setLong(1, userId);
            stmt.setString(2, messageText);
            stmt.executeUpdate();
            return true;
        } catch (Exception e) {
            logger.severe("Ошибка при добавлении сообщения в базу данных: " + e.getMessage());
            return false;
        }
    }

    private void markMessageAsSent(Long userId, String messageText) {
        try (Connection connection = DriverManager.getConnection(dbUrl, user, password);
             PreparedStatement stmt = connection.prepareStatement("UPDATE messages SET sent = true WHERE user_id = ? AND message = ?")) {
            stmt.setLong(1, userId);
            stmt.setString(2, messageText);
            stmt.executeUpdate();
        } catch (Exception e) {
            logger.severe("Ошибка при обновлении статуса сообщения в базе данных: " + e.getMessage());
        }
    }

    private void recoverMessages() {
        try (Connection connection = DriverManager.getConnection(dbUrl, user, password);
             PreparedStatement stmt = connection.prepareStatement("SELECT user_id, message FROM messages WHERE sent = false");
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                Long userId = rs.getLong("user_id");
                String messageText = rs.getString("message");
                SendMessage message = new SendMessage();
                message.setChatId(String.valueOf(userId));
                message.setText(messageText);
                queueMessage(userId, message);
            }
        } catch (Exception e) {
            logger.severe("Ошибка при восстановлении сообщений из базы данных: " + e.getMessage());
        }
    }
}
