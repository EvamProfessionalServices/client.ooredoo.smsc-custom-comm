package com.evam.marketing.communication.template.configuration.kafka.property;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Created by cemserit on 3.03.2021.
 */
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties("kafka")
@Data
public class KafkaProperties {
    private String bootstrapAddress;
    private boolean communicationUUIDCheck;
    private KafkaTopicProperty eventTopic;
    private KafkaTopicProperty integrationTopic;
    private KafkaProducerProperty producer;

}
