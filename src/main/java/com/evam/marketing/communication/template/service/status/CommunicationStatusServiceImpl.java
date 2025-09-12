package com.evam.marketing.communication.template.service.status;

import com.evam.marketing.communication.template.repository.status.CommunicationStatusRepository;
import com.evam.marketing.communication.template.repository.status.model.CustomCommunicationStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by cemserit on 5.04.2021.
 */
@Service
@Slf4j
public class CommunicationStatusServiceImpl implements CommunicationStatusService {

    private final CommunicationStatusRepository communicationStatusRepository;

    public CommunicationStatusServiceImpl(CommunicationStatusRepository communicationStatusRepository) {
        this.communicationStatusRepository = communicationStatusRepository;
    }

    @Override
    public Set<String> getCommunicationStatus(Collection<String> communicationUUIDList) {
        return communicationStatusRepository.findByCommunicationUUIDIn(communicationUUIDList)
                .stream()
                .map(CustomCommunicationStatus::getCommunicationUUID)
                .collect(Collectors.toSet());
    }

    @Override
    public void saveBatchCommunicationStatus(Collection<CustomCommunicationStatus> statusList) {
        List<CustomCommunicationStatus> responseStatus = communicationStatusRepository.saveAll(statusList);
        log.info("Communication status successfully batch saved. Request: {}, response: {}",
                statusList, responseStatus);
    }
}
