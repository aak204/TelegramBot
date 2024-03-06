package telegram_bot.messaging;

import org.telegram.telegrambots.meta.api.methods.send.SendMessage;

public record MessageEvent(Long userId, SendMessage message) {
}
