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
}
