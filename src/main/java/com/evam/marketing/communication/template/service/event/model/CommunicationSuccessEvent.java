package com.evam.marketing.communication.template.service.event.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Created by cemserit on 2.03.2021.
 */
@Getter
@SuperBuilder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class CommunicationSuccessEvent extends AbstractCommunicationResponseEvent {

    @Override
    public String getName() {
        return CommunicationEventName.SUCCESS.getEventName();
    }

    @Override
    public CustomCommunicationEventType getType() {
        return CustomCommunicationEventType.SUCCESS;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }
}
