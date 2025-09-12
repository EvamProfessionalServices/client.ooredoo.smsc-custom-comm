package com.evam.marketing.communication.template.service.client;

import com.evam.marketing.communication.template.service.event.KafkaProducerService;
import com.evam.marketing.communication.template.service.event.model.CommunicationResponseEvent;
import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;

public abstract class AbstractCommunicationClient implements CommunicationClient {

    private final KafkaProducerService kafkaProducerService;

    protected AbstractCommunicationClient(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    protected CommunicationResponse generateSuccessCommunicationResponse(
            CommunicationRequest communicationRequest, String providerResponseId, String message) {
        return CommunicationResponse.builder()
                .success(true)
                .communicationCode(communicationRequest.getCommunicationCode())
                .communicationUUID(communicationRequest.getCommunicationUUID())
                .actorId(communicationRequest.getActorId())
                .scenario(communicationRequest.getScenario())
                .provider(getProvider())
                .providerResponseId(providerResponseId)
                .message(message)
                .build();
    }

    protected CommunicationResponse generateFailCommunicationResponse(
            CommunicationRequest communicationRequest, String message, String reason) {
        return CommunicationResponse.builder()
                .success(false)
                .communicationCode(communicationRequest.getCommunicationCode())
                .communicationUUID(communicationRequest.getCommunicationUUID())
                .actorId(communicationRequest.getActorId())
                .scenario(communicationRequest.getScenario())
                .provider(getProvider())
                .message(message)
                .reason(reason)
                .build();
    }

    @Deprecated
    void sendFailEvent(CommunicationResponse communicationResponse) {
        CommunicationResponseEvent communicationResponseEvent = communicationResponse.toEvent();
        kafkaProducerService.sendEvent(communicationResponseEvent);
    }

    @Deprecated
    void sendSuccessEvent(CommunicationResponse communicationResponse) {
        CommunicationResponseEvent communicationResponseEvent = communicationResponse.toEvent();
        kafkaProducerService.sendEvent(communicationResponseEvent);
    }

    public void sendEvent(CommunicationResponseEvent event) {
        kafkaProducerService.sendEvent(event);
    }
}
