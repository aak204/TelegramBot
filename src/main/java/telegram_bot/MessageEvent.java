package telegram_bot;

import org.telegram.telegrambots.meta.api.methods.send.SendMessage;

public record MessageEvent(Long userId, SendMessage message) {
}
