package telegram.bot.TelegramBot;

import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;

import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class MessageDistributor {
    private final Map<Long, BlockingQueue<SendMessage>> userMessageQueues = new ConcurrentHashMap<>();
    private final Map<Long, ScheduledExecutorService> userSchedulers = new ConcurrentHashMap<>();
    private final TelegramLongPollingBot bot;
    private static final Logger logger = Logger.getLogger(MessageDistributor.class.getName());

    public MessageDistributor(final TelegramLongPollingBot bot) {
        this.bot = bot;
    }

    public void queueMessage(final Long userId, final SendMessage message) {
        userMessageQueues.computeIfAbsent(userId, k -> {
            BlockingQueue<SendMessage> queue = new LinkedBlockingQueue<>();
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleWithFixedDelay(() -> {
                SendMessage msg = queue.poll();
                if (msg != null) {
                    try {
                        bot.executeAsync(msg);
                    } catch (Exception e) {
                        logger.severe("Исключение при отправке сообщения пользователю: " + userId + ", ошибка: " + e.getMessage());
                    }
                }
            }, 0, 50, TimeUnit.MILLISECONDS);
            userSchedulers.put(userId, scheduler);
            return queue;
        }).offer(message);
    }

}
