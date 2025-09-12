package com.evam.marketing.communication.template.service.event.model;

import lombok.Getter;

/**
 * Created by cemserit on 4.03.2021.
 */
public enum CommunicationEventName {
    SUCCESS("customCommunicationSuccess"), FAIL("customCommunicationFail");

    @Getter
    private String eventName;

    CommunicationEventName(String eventName) {
        this.eventName = eventName;
    }
}
