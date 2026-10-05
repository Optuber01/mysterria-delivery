package net.mysterria.delivery.command;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import net.mysterria.delivery.MysterriaDelivery;
import net.mysterria.delivery.config.DeliveryConfig;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DeliveryCommand implements CommandExecutor, TabCompleter {
    
    private final MysterriaDelivery plugin;
    
    public DeliveryCommand(MysterriaDelivery plugin) {
        this.plugin = plugin;
    }
    
    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, @NotNull String[] args) {
        if (!sender.hasPermission("mysterria.delivery.admin")) {
            sender.sendMessage(Component.text("You don't have permission to use this command.", NamedTextColor.RED));
            return true;
        }
        
        if (args.length == 0) {
            sender.sendMessage(Component.text("Usage: /delivery reload", NamedTextColor.YELLOW));
            return true;
        }
        
        if (args[0].equalsIgnoreCase("reload")) {
            DeliveryConfig before = plugin.getDeliveryConfig();
            String commandText = label + " " + String.join(" ", args);
            try {
                plugin.reload();
            } catch (RuntimeException failure) {
                Map<String, Object> facts = configChange(before, plugin.getDeliveryConfig());
                facts.put("reason", failure.getClass().getSimpleName());
                plugin.getAuditEmitter().emitAdmin("config-reload", AuditOutcome.FAILED, AuditRisk.HIGH,
                        sender, commandText, facts);
                throw failure;
            }
            plugin.getAuditEmitter().emitAdmin("config-reload", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                    sender, commandText, configChange(before, plugin.getDeliveryConfig()));
            sender.sendMessage(Component.text("MysterriaDelivery configuration reloaded!", NamedTextColor.GREEN));
            return true;
        }
        
        sender.sendMessage(Component.text("Unknown subcommand. Use: /delivery reload", NamedTextColor.RED));
        return true;
    }
    
    private static Map<String, Object> configChange(DeliveryConfig before, DeliveryConfig after) {
        Map<String, Object> facts = new LinkedHashMap<>();
        facts.put("old_announcements_enabled", before.isAnnouncementsEnabled());
        facts.put("new_announcements_enabled", after.isAnnouncementsEnabled());
        facts.put("old_announcement_global", before.isAnnouncementGlobal());
        facts.put("new_announcement_global", after.isAnnouncementGlobal());
        facts.put("old_max_retries", before.getMaxRetries());
        facts.put("new_max_retries", after.getMaxRetries());
        facts.put("old_delivery_delay_ticks", before.getDeliveryDelayTicks());
        facts.put("new_delivery_delay_ticks", after.getDeliveryDelayTicks());
        return facts;
    }

    @Override
    public List<String> onTabComplete(@NotNull CommandSender sender, @NotNull Command command, @NotNull String alias, @NotNull String[] args) {
        if (args.length == 1) {
            return List.of("reload");
        }
        return List.of();
    }
}