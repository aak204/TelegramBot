package telegram.bot.TelegramBot;

import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
import java.util.HashSet;
import java.util.Set;

public class NotificationBot extends TelegramLongPollingBot {
    private final String BOT_USERNAME = "Test190NotificationsBot";
    private final String BOT_TOKEN = "6833871789:AAE5jqbLTlEAiPlPJAtYS8yvVR9OEsOEcMQ";
    private final MessageDistributor distributor;
    private final Set<Long> chatIds = new HashSet<>();

    public NotificationBot() {
        this.distributor = new MessageDistributor(this);
    }

    @Override
    public String getBotUsername() {
        return BOT_USERNAME;
    }

    @Override
    public String getBotToken() {
        return BOT_TOKEN;
    }

    @Override
    public void onUpdateReceived(final Update update) {
        if (update.hasMessage() && update.getMessage().hasText()) {
            String messageText = update.getMessage().getText();
            long chatId = update.getMessage().getChatId();
            chatIds.add(chatId); // Добавляем ID чата в список, если получено сообщение

            if (messageText.equalsIgnoreCase("отправить")) {
                chatIds.parallelStream().forEach(id -> {
                    for (int i = 0; i < 100; i++) {
                        SendMessage message = new SendMessage();
                        message.setChatId(String.valueOf(id));
                        message.setText(String.format("Уведомление отправлено %s", i + 1));
                        distributor.queueMessage(id, message);
                    }
                });
            }
        }
    }
}
