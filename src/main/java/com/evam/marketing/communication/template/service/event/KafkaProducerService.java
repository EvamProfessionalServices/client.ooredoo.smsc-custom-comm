package com.evam.marketing.communication.template.service.event;

import com.evam.marketing.communication.template.configuration.kafka.property.KafkaProperties;
import com.evam.marketing.communication.template.service.event.model.CommunicationResponseEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by cemserit on 2.03.2021.
 */
@Service
@Slf4j
public class KafkaProducerService {

  private final KafkaTemplate<String, CommunicationResponseEvent> kafkaTemplate;
  private final KafkaProperties kafkaProperties;

  public KafkaProducerService(KafkaTemplate<String, CommunicationResponseEvent> kafkaTemplate,
      KafkaProperties kafkaProperties) {
    this.kafkaTemplate = kafkaTemplate;
    this.kafkaProperties = kafkaProperties;
  }

  public void sendEvent(CommunicationResponseEvent event) {
    String eventName = kafkaProperties.getEventTopic().getName();
    ProducerRecord<String, CommunicationResponseEvent> rec = new ProducerRecord<>
        (eventName, event.getActorId(), event);

    kafkaTemplate.send(rec).whenComplete((res, exception) -> {
      if (exception == null) {
        if (log.isTraceEnabled()) {
          log.trace("Kafka message successfully sent.");
        } else {
          log.debug("Kafka message successfully sent.");
        }
      } else {
        log.warn("Kafka message send fail!", exception);
      }
    });
  }

  public void sendEvents(List<CommunicationResponseEvent> events) {
    events.forEach(this::sendEvent);
  }
}
