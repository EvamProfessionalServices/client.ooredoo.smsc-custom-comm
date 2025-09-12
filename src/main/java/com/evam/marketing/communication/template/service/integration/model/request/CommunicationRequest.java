package com.evam.marketing.communication.template.service.integration.model.request;

import java.io.Serializable;

/**
 * Created by cemserit on 13.04.2021.
 */
public interface CommunicationRequest extends Serializable {
    String getMessageType();

    String getActorId();

    String getCommunicationCode();

    String getCommunicationUUID();

    String getScenario();

    boolean isTransactional();
}
