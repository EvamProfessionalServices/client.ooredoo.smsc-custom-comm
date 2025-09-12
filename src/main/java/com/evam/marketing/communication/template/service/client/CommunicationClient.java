package com.evam.marketing.communication.template.service.client;

import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;
import jakarta.validation.constraints.NotNull;

/**
 * Created by cemserit on 12.03.2021.
 */
public interface CommunicationClient {

    @NotNull
    CommunicationResponse send(CommunicationRequest communicationRequest);

    String getProvider();
}
