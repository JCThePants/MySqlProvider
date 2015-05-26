package com.jcwhatever.nucleus.providers.mysql;

import com.jcwhatever.nucleus.Nucleus;
import com.jcwhatever.nucleus.managed.messaging.ChatPaginator;
import com.jcwhatever.nucleus.managed.messaging.IMessenger;
import com.jcwhatever.nucleus.utils.text.TextUtils;

import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;

import java.util.Collection;
import java.util.UUID;

/**
 * Convenience class for accessing musical regions chat and console messenger.
 */
public class Msg {

    private Msg() {}

    public static void tell(CommandSender sender, String message, Object...params) {
        msg().tell(sender, message, params);
    }

    public static void tell(Player p, String message, Object...params) {
        msg().tell(p, message, params);
    }

    public static void tellNoSpam(Player p, String message, Object...params) {
        msg().tellNoSpam(p, message, params);
    }

    public static void tellImportant(UUID playerId, String context, String message, Object...params) {
        msg().tellImportant(playerId, context, message, params);
    }

    public static void info(String message, Object...params) {
        msg().info(message, params);
    }

    public static void debug(String message, Object...params) {
        if (!Nucleus.getPlugin().isDebugging())
            return;

        msg().debug(message, params);
    }

    public static void warning(String message, Object...params) {
        msg().warning(message, params);
    }

    public static void severe(String message, Object...params) {
        msg().severe(message, params);
    }

    public static void broadcast(String message, Object...params) {
        msg().broadcast(message, params);
    }

    public static void broadcast(String message, Collection<Player> exclude, Object...params) {
        msg().broadcast(exclude, message, params);
    }

    public static ChatPaginator getPaginator(String title, Object...params) {
        return new ChatPaginator(Nucleus.getPlugin(), 6, TextUtils.format(title, params));
    }

    private static IMessenger msg() {
        return Nucleus.getPlugin().getMessenger();
    }
}
