package com.evam.marketing.communication.template.configuration.kafka.property;

import lombok.Data;

import java.util.Collections;
import java.util.List;

/**
 * Created by cemserit on 3.03.2021.
 */
@Data
public class KafkaTopicProperty {
    private String name;
    private String group;
    private int partition;
    private String autoOffsetReset;
    private boolean enableAutoCommit;
    private int concurrency;
    private short replication;
    private String retentionMs;
    private List<KafkaCustomProperty> customConfigProperties = Collections.emptyList();
    private int maxPoolRecords = 1000;
}
