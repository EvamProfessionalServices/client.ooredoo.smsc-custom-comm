package com.evam.marketing.communication.template.service.stream;

import com.evam.marketing.communication.template.service.integration.CommunicationService;
import com.evam.marketing.communication.template.service.stream.model.request.StreamRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by cemserit on 3.03.2021.
 */
@Component
@Slf4j
public class IntegrationKafkaConsumer {
    public static final String LISTENER_ID = "INTEGRATION_LISTENER";

    private final CommunicationService communicationService;

    public IntegrationKafkaConsumer(CommunicationService communicationService) {
        this.communicationService = communicationService;
    }

    @KafkaListener(id = LISTENER_ID,
            topics = {"${kafka.integration-topic.name}"},
            groupId = "${kafka.integration-topic.group}",
            containerFactory = "integrationKafkaListenerContainerFactory"
    )
    public void integrationListener(List<StreamRequest> requestList, Acknowledgment ack) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Received communication request records [{}]. {}", requestList.size(), requestList);
            } else {
                log.info("Received communication request records [{}]", requestList.size());
            }

            communicationService.execute(requestList);
        } catch (Exception e) {
            log.error("An error occurred while processing requests", e);
        } finally {
            ack.acknowledge();
        }
    }
}
