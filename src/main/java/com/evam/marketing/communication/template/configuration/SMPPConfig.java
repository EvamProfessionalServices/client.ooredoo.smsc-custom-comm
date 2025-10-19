package com.evam.marketing.communication.template.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "smpp")
@Data
public class SMPPConfig {
    private String host;
    private int port;
    private String username;
    private String password;
    private String type;
    private long timeout;

    private Batch batch = new Batch();

    @Data
    public static class Batch {
        private int maxRetries= 2; // Varsayılan değerler
        private int size= 200;
        private long flushIntervalMs= 200;
    }
    // YENİ EKLENECEK ALAN
    private DeliveryReport deliveryReport;

    // ...

    // YENİ EKLENECEK INNER CLASS
    @Data // veya Getter/Setter
    public static class DeliveryReport {
        private String tableName;
    }
}
