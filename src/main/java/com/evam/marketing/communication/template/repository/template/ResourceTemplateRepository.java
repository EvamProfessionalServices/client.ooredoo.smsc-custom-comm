package com.evam.marketing.communication.template.repository.template;

import com.evam.marketing.communication.template.repository.template.model.ResourceTemplate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Created by cemserit on 16.02.2021.
 */
@Repository
public interface ResourceTemplateRepository extends JpaRepository<ResourceTemplate, Long> {
    Optional<ResourceTemplate> findByCommunicationCodeAndScenarioNameAndScenarioVersion(String communicationCode,
                                                                                        String scenarioName,
                                                                                        int scenarioVersion);
}
