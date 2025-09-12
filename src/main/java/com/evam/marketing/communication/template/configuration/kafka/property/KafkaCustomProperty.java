package com.evam.marketing.communication.template.configuration.kafka.property;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Created by cemserit on 2.03.2021.
 */
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class KafkaCustomProperty {
    private String key;
    private String value;
}
