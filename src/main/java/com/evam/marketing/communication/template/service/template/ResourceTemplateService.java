package com.evam.marketing.communication.template.service.template;

import com.evam.marketing.communication.template.repository.template.model.ResourceTemplate;

import java.util.Optional;

/**
 * Created by cemserit on 16.02.2021.
 */
public interface ResourceTemplateService {
    Optional<ResourceTemplate> getResourceTemplate(String communicationCode, String scenarioName, int scenarioVersion);
}
