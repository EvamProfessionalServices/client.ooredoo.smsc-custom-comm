package com.evam.marketing.communication.template.service.integration;

import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;
import com.evam.marketing.communication.template.service.stream.model.request.StreamRequest;

import java.util.List;

/**
 * Created by cemserit on 11.03.2021.
 */
public interface CommunicationService {
    List<CommunicationResponse> execute(List<StreamRequest> requestList);
}
