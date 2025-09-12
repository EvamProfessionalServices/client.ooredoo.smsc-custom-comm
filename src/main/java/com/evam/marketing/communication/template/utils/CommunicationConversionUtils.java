package com.evam.marketing.communication.template.utils;

import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.stream.model.request.CustomCommunicationStreamRequest;
import com.evam.marketing.communication.template.service.stream.model.request.StreamRequest;

/**
 * Created by cemserit on 15.04.2021.
 */
public final class CommunicationConversionUtils {
    private CommunicationConversionUtils() {
    }

    public static CommunicationRequest streamRequestToCommunicationRequest(StreamRequest request, String content) {
        CustomCommunicationStreamRequest customCommunicationStreamRequest = (CustomCommunicationStreamRequest) request;

        return CustomCommunicationRequest.builder()
                .messageType(customCommunicationStreamRequest.getMessageType())
                .actorId(customCommunicationStreamRequest.getActorId())
                .communicationCode(customCommunicationStreamRequest.getCode())
                .communicationUUID(customCommunicationStreamRequest.getUuid())
                .scenario(customCommunicationStreamRequest.getScenario())
                .scenarioVersion(customCommunicationStreamRequest.getScenarioVersion())
                .parameters(customCommunicationStreamRequest.getParameters())
                .build();
    }
}
