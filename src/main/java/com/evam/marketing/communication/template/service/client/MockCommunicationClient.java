package com.evam.marketing.communication.template.service.client;

import com.evam.evamperformancereport.PerformanceCounter;
import com.evam.evamperformancereport.annotation.Performance;
import com.evam.evamperformancereport.model.PerformanceModelType;
import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.event.KafkaProducerService;
import com.evam.marketing.communication.template.service.event.model.CommunicationResponseEvent;
import com.evam.marketing.communication.template.service.exception.ServiceException;
import com.evam.marketing.communication.template.service.integration.HandlingService;
import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by cemserit on 12.03.2021.
 */
@Service
@Slf4j
@Performance(type = {PerformanceModelType.SUCCESS, PerformanceModelType.ERROR})
public class MockCommunicationClient extends AbstractCommunicationClient {

    private static final String PROVIDER = "SMS";
    private static final SecureRandom RANDOM = new SecureRandom();
    private final PerformanceCounter performanceCounter;
    private static final String CLASS_SIMPLE_NAME = MockCommunicationClient.class.getSimpleName();
    private final HandlingService handlingService;

    @Autowired
    public MockCommunicationClient(KafkaProducerService kafkaProducerService,
        PerformanceCounter performanceCounter, HandlingService handlingService) {
        super(kafkaProducerService);
        this.performanceCounter = performanceCounter;
        this.handlingService = handlingService;
    }

    @Override
    @RateLimiter(name = "client-limiter")
    @NotNull
    public CommunicationResponse send(CommunicationRequest communicationRequest) {
        try {
            CustomCommunicationRequest customCommunicationRequest = (CustomCommunicationRequest) communicationRequest;

            log.info("Mock communication sender worked. Time: {}, Request: {}",
                    LocalDateTime.now(), customCommunicationRequest);
            handlingService.handleRequest(customCommunicationRequest);
            CommunicationResponse communicationResponse = generateCommunicationResponse(
                    communicationRequest);

//            CommunicationResponseEvent communicationResponseEvent = communicationResponse.toEvent();
//            communicationResponseEvent.addCustomParameter("stringKey1", "v1");
//            communicationResponseEvent.addCustomParameter("numericKey1", BigDecimal.valueOf(44));
//            sendEvent(communicationResponseEvent);
//            performanceCounter.increment(CLASS_SIMPLE_NAME, PerformanceModelType.SUCCESS);
            return communicationResponse;
        } catch (ServiceException e) {
            log.error("An unexpected error occurred. Request: {}",
                    communicationRequest, e);
            CommunicationResponse communicationResponse = generateFailCommunicationResponse(
                    communicationRequest, e.getMessage(), "UNEXPECTED");
            sendEvent(communicationResponse.toEvent());
            performanceCounter.increment(CLASS_SIMPLE_NAME, PerformanceModelType.ERROR);
            return communicationResponse;
        }
    }

    @Override
    public String getProvider() {
        return PROVIDER;
    }

    private CommunicationResponse generateCommunicationResponse(
            CommunicationRequest communicationRequest) throws ServiceException{
        return generateSuccessCommunicationResponse(communicationRequest,
                getProvider(), "Success Message");
    }
}
