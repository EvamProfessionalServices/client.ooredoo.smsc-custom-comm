package com.evam.marketing.communication.template.configuration.kafka.property;

import lombok.Data;

import java.util.Collections;
import java.util.List;

/**
 * Created by cemserit on 8.03.2021.
 */
@Data
public class KafkaProducerProperty {
    private long bufferMemory = 33554432;
    private int batchSize = 16384;
    private long lingerMs;
    private String acks = "1";
    private List<KafkaCustomProperty> customConfigProperties = Collections.emptyList();
}
