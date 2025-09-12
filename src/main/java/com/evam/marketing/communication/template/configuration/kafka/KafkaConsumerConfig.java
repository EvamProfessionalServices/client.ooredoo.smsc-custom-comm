package com.evam.marketing.communication.template.configuration.kafka;

import com.evam.marketing.communication.template.configuration.kafka.property.KafkaProperties;
import com.evam.marketing.communication.template.utils.SerializationUtils;
import com.evam.marketing.communication.template.service.stream.model.request.StreamRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cemserit on 7.03.2021.
 */
@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final ObjectMapper objectMapper = SerializationUtils.generateObjectMapper();

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StreamRequest> integrationKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, StreamRequest> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(integrationConsumerFactory());
        factory.setConcurrency(kafkaProperties.getIntegrationTopic().getConcurrency());
        factory.setBatchListener(true);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        return factory;
    }

    private Map<String, Object> integrationConsumerConfigs() {
        Map<String, Object> configProps = new HashMap<>();

        kafkaProperties.getIntegrationTopic()
                .getCustomConfigProperties()
                .forEach(kafkaConfigProperty -> configProps.put(kafkaConfigProperty.getKey(),
                        kafkaConfigProperty.getValue()));

        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapAddress());
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getIntegrationTopic().getGroup());
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaProperties.getIntegrationTopic().getAutoOffsetReset());
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaProperties.getIntegrationTopic().isEnableAutoCommit());
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaProperties.getIntegrationTopic().getMaxPoolRecords());
        return configProps;
    }

    private ConsumerFactory<String, StreamRequest> integrationConsumerFactory() {
        Map<String, Object> firebaseConsumerConfigs = integrationConsumerConfigs();
        StringDeserializer keyDeserializer = new StringDeserializer();
        JsonDeserializer<StreamRequest> valueDeserializer = new JsonDeserializer<>(StreamRequest.class,
                objectMapper, false);
        return new DefaultKafkaConsumerFactory<>(firebaseConsumerConfigs, keyDeserializer, valueDeserializer);
    }
}
