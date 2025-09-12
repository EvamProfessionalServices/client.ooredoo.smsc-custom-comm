package com.evam.marketing.communication.template.service.template;

import com.evam.marketing.communication.template.repository.template.ResourceTemplateRepository;
import com.evam.marketing.communication.template.repository.template.model.ResourceTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

/**
 * Created by cemserit on 16.02.2021.
 */
@Service
@Slf4j
public class ResourceTemplateServiceImpl implements ResourceTemplateService {
    private final ResourceTemplateRepository resourceTemplateRepository;
    public static final String RESOURCE_TEMPLATE_CACHE_KEY = "resource-template-cache";

    public ResourceTemplateServiceImpl(ResourceTemplateRepository resourceTemplateRepository) {
        this.resourceTemplateRepository = resourceTemplateRepository;
    }

    @Cacheable(value = ResourceTemplateServiceImpl.RESOURCE_TEMPLATE_CACHE_KEY, unless = "#result == null or #result == \"\"")
    @Transactional
    @Override
    public Optional<ResourceTemplate> getResourceTemplate(String communicationCode, String scenarioName, int scenarioVersion) {
        return resourceTemplateRepository.findByCommunicationCodeAndScenarioNameAndScenarioVersion(communicationCode, scenarioName, scenarioVersion);
    }
}
