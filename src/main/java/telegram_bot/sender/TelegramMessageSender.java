package telegram_bot.sender;

import com.rabbitmq.client.Channel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.meta.api.methods.send.SendMessage;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import java.io.IOException;
import java.util.logging.Logger;

@Component
public class TelegramMessageSender {
    private final TelegramLongPollingBot bot;

    @Autowired
    public TelegramMessageSender(TelegramLongPollingBot bot) {
        this.bot = bot;
    }

    public void sendTelegramMessage(final Long userId, final SendMessage message, final long deliveryTag, final Channel channel) {
        try {
            bot.executeAsync(message).thenAccept(msg -> {
                try {
                    channel.basicAck(deliveryTag, false); // Подтверждаем после успешной отправки
                } catch (IOException usageError) {
                    Logger.getLogger(TelegramMessageSender.class.getName()).severe(String.format("Ошибка при подтверждении сообщения для пользователя: %d, ошибка: %s", userId, usageError.getMessage()));
                }
            }).exceptionally(sendingError -> {
                try {
                    // При неудачной отправке сообщения отправляем basicNack, чтобы сообщение могло быть повторно обработано
                    channel.basicNack(deliveryTag, false, true);
                } catch (IOException nackError) {
                    Logger.getLogger(TelegramMessageSender.class.getName()).severe(String.format("Ошибка при возврате сообщения в очередь для пользователя: %d, ошибка: %s", userId, nackError.getMessage()));
                }
                return null;
            });
        } catch (Exception error) {
            Logger.getLogger(TelegramMessageSender.class.getName()).severe(String.format("Исключение при отправке сообщения пользователю: %d, ошибка: %s", userId, error.getMessage()));
        }
    }

}
