package com.evam.marketing.communication.template.service.event.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Created by cemserit on 2.03.2021.
 */
@SuperBuilder
@Getter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CommunicationFailEvent extends AbstractCommunicationResponseEvent {
    private String reason;

    @Override
    public String getName() {
        return CommunicationEventName.FAIL.getEventName();
    }

    @Override
    public CustomCommunicationEventType getType() {
        return CustomCommunicationEventType.FAIL;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }
}
