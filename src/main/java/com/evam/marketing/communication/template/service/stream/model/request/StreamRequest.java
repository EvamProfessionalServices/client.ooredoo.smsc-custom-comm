package com.evam.marketing.communication.template.service.stream.model.request;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;

/**
 * Created by cemserit on 3.03.2021.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
        defaultImpl = CustomCommunicationStreamRequest.class)
public interface StreamRequest {
    String getName();

    String getCode();

    String getUuid();

    String getScenario();

    int getScenarioVersion();

    String getActorId();

    String getType();

    String getMessageType();

    Map<String, Object> getResourceVariables();

    boolean hasResource();
}
