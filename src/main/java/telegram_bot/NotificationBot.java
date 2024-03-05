package telegram_bot;

import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.meta.api.objects.Update;
public class NotificationBot extends TelegramLongPollingBot {
    private final MessageDistributor distributor;

    public NotificationBot() {
        this.distributor = new MessageDistributor(this);
    }

    @Override
    public String getBotUsername() {
        return "Test190NotificationsBot";
    }

    @Override
    public String getBotToken() {
        return "6833871789:AAE5jqbLTlEAiPlPJAtYS8yvVR9OEsOEcMQ";
    }

    @Override
    public void onUpdateReceived(final Update update) {
        if (update.hasMessage() && update.getMessage().hasText()) {
            String messageText = update.getMessage().getText();
            long chatId = update.getMessage().getChatId();

            if (messageText.equalsIgnoreCase("отправить")) {
                for (int i = 0; i < 100; i++) {
                    SendMessage message = new SendMessage();
                    message.setChatId(String.valueOf(chatId)); // Отправляем сообщение только этому chatId
                    message.setText(String.format("Уведомление отправлено %s", i + 1));
                    distributor.queueMessage(chatId, message);
                }
            }
        }
    }
}
