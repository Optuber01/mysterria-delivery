package net.mysterria.delivery;

import dev.ua.ikeepcalm.catwalk.hub.webserver.services.CatWalkWebserverService;
import lombok.Getter;
import net.luckperms.api.LuckPerms;
import net.mysterria.delivery.api.*;
import net.mysterria.delivery.audit.DeliveryAuditEmitter;
import net.mysterria.delivery.command.DeliveryCommand;
import net.mysterria.delivery.config.DeliveryConfig;
import net.mysterria.delivery.listener.PlayerJoinListener;
import net.mysterria.delivery.manager.DeliveryManager;
import net.mysterria.delivery.manager.QueueManager;
import net.mysterria.delivery.manager.TranslationManager;
import org.bukkit.Bukkit;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

@Getter
public class MysterriaDelivery extends JavaPlugin {

    @Getter
    private static MysterriaDelivery instance;

    private DeliveryConfig deliveryConfig;
    private DeliveryManager deliveryManager;
    private QueueManager queueManager;
    private TranslationManager translationManager;
    private LuckPerms luckPerms;
    private DeliveryAuditEmitter auditEmitter;

    @Override
    public void onEnable() {
        instance = this;

        saveDefaultConfig();
        saveResource("translations/en.yml", false);
        saveResource("translations/uk.yml", false);

        if (!setupLuckPerms()) {
            getLogger().severe("LuckPerms not found! Disabling plugin.");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        deliveryConfig = new DeliveryConfig(this);
        translationManager = new TranslationManager(this);
        auditEmitter = new DeliveryAuditEmitter(this);
        queueManager = new QueueManager(this, auditEmitter);
        deliveryManager = new DeliveryManager(this, deliveryConfig, queueManager);
        queueManager.loadQueue();

        getCommand("delivery").setExecutor(new DeliveryCommand(this));

        getServer().getPluginManager().registerEvents(new PlayerJoinListener(queueManager, deliveryManager), this);

        CatWalkWebserverService webserverService = Bukkit.getServicesManager().load(CatWalkWebserverService.class);
        if (webserverService == null) {
            getLogger().severe("Failed to load CatWalkWebserverService. Disabling plugin.");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        webserverService.registerHandlers(new ItemDeliveryEndpoint(deliveryManager));
        webserverService.registerHandlers(new SubscriptionDeliveryEndpoint(deliveryManager));
        webserverService.registerHandlers(new PermissionDeliveryEndpoint(deliveryManager));
        webserverService.registerHandlers(new VoteRewardDeliveryEndpoint(deliveryManager));

        getLogger().info("MysterriaDelivery has been enabled!");
    }

    @Override
    public void onDisable() {
        if (auditEmitter != null) {
            auditEmitter.close();
        }
        if (queueManager != null) {
            queueManager.saveQueue();
        }
        getLogger().info("MysterriaDelivery has been disabled!");
    }

    private boolean setupLuckPerms() {
        RegisteredServiceProvider<LuckPerms> provider = Bukkit.getServicesManager().getRegistration(LuckPerms.class);
        if (provider != null) {
            luckPerms = provider.getProvider();
            return true;
        }
        return false;
    }

    public void reload() {
        reloadConfig();
        deliveryConfig = new DeliveryConfig(this);
        translationManager.reload();
        deliveryManager.reload(deliveryConfig);
    }
}
