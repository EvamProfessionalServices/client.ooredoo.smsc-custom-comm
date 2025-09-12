package com.evam.marketing.communication.template.repository.status;

import com.evam.marketing.communication.template.repository.status.model.CustomCommunicationStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Set;

/**
 * Created by cemserit on 5.03.2021.
 */
@Repository
public interface CommunicationStatusRepository extends JpaRepository<CustomCommunicationStatus, Long> {
    Set<CustomCommunicationStatus> findByCommunicationUUIDIn(Collection<String> communicationUUIDSet);
}
