package com.evam.marketing.communication.template.service.status;

import com.evam.marketing.communication.template.repository.status.model.CustomCommunicationStatus;

import java.util.Collection;
import java.util.Set;

/**
 * Created by cemserit on 5.04.2021.
 */
public interface CommunicationStatusService {
    Set<String> getCommunicationStatus(Collection<String> communicationUUIDList);

    void saveBatchCommunicationStatus(Collection<CustomCommunicationStatus> statusList);
}
