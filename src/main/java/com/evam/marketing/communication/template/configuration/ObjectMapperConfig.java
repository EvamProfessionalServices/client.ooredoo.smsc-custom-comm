package com.evam.marketing.communication.template.configuration;

import com.evam.marketing.communication.template.utils.SerializationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Created by cemserit on 24.02.2021.
 */
@Configuration
public class ObjectMapperConfig {
    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        return SerializationUtils.generateObjectMapper();
    }
}
