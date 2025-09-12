package com.evam.marketing.communication.template.service.integration;

import com.evam.marketing.communication.template.configuration.PersistentConfig;
import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.client.model.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Service
@Slf4j
public class HandlingService {
    private final PersistentConfig persistentConfig;
    private final PersistenceService persistenceService;
    private final String CONTROL_GROUP = "CONTROL_GROUP";

    public HandlingService(PersistentConfig persistentConfig, PersistenceService persistenceService) {
        this.persistentConfig = persistentConfig;
        this.persistenceService = persistenceService;
    }
    public void handleRequest(CustomCommunicationRequest customCommunicationRequest) {

        try {
            ZonedDateTime now = ZonedDateTime.now();

            ServiceRequest serviceRequest = getServiceRequest(now, customCommunicationRequest);

            List<String> testActors = Arrays.asList(persistentConfig.getTestActors().split(","));
            log.info("Test actors from config: {}", testActors);
            if(testActors.contains(customCommunicationRequest.getActorId())){
                serviceRequest.getComputed().setTestModeEnabled(false);
                log.info("Test actor detected. ActorId: {}", customCommunicationRequest.getActorId());
                log.info("Test mode : {}",serviceRequest.getComputed().isTestModeEnabled());
            }
            log.info("Actor id {}", customCommunicationRequest.getActorId());
            persistenceService.enqueue(serviceRequest);
        } catch (Exception e) {
            log.error("Error occurred while handling request: {}", customCommunicationRequest, e);
        }

    }

    private ServiceRequest getServiceRequest(ZonedDateTime now, CustomCommunicationRequest request) {
        ServiceRequest.ServiceRequestBuilder builder = ServiceRequest.builder();
        builder.originalRequest(request);

        ServiceRequest.Base base = getBase(request);
        builder.base(base);

        ServiceRequest.Computed computed = getComputed(now, request);
        builder.computed(computed);

        return builder.build();
    }

    private ServiceRequest.Base getBase(CustomCommunicationRequest request) {
        return ServiceRequest.Base.builder()
                .actorId(request.getActorId())
                .communication_uuid(request.getCommunicationUUID())
                .scenarioName(request.getScenario())
                .communication_code(request.getCommunicationCode())
                .communication_name(request.getScenario())
                .build();
    }

    private ServiceRequest.Computed getComputed(ZonedDateTime now, CustomCommunicationRequest request) {
        ServiceRequest.Computed.ComputedBuilder computedBuilder = ServiceRequest.Computed.builder();
        computedBuilder.quotaCheck(request.getQuotaCheck());
        for (Parameter parameter : request.getParameters()){
            switch (parameter.getName()){
                case "testModeEnabled":
                    if(CONTROL_GROUP.equals(parameter.getValue())){
                        computedBuilder.controlGroup(true);
                        computedBuilder.testModeEnabled(false);
                    }else{
                        computedBuilder.controlGroup(false);
                        computedBuilder.testModeEnabled(Boolean.parseBoolean(parameter.getValue()));
                    }
                    break;
                case "To":
                    computedBuilder.to(parameter.getValue());
                    break;
                case "SenderId":
                    computedBuilder.senderId(parameter.getValue());
                    break;
                case "LanguagePreference":
                    computedBuilder.languagePreference(parameter.getValue());
                    break;
                case "Text_Eng":
                    computedBuilder.textEng(parameter.getValue());
                    break;
                case "Text_Ar":
                    computedBuilder.textAr(parameter.getValue());
                    break;
                case "Text_Mixed":
                    computedBuilder.textMixed(parameter.getValue());
                    break;
                case "flashSmsEnabled":
                    computedBuilder.flashSmsEnabled(Boolean.parseBoolean(parameter.getValue()));
                    break;
                case "segmentName":
                    computedBuilder.segmentName(parameter.getValue());
                    break;
                case "camId":
                    computedBuilder.camId(parameter.getValue());
                    break;
                case "messageType":
                    computedBuilder.messageType(parameter.getValue());
                    break;
                case "contractId":
                    computedBuilder.contractId(parameter.getValue());
                    break;
                case "transactionId":
                    computedBuilder.transactionId(parameter.getValue());
                    break;
                case "treatmentName":
                    computedBuilder.treatmentName(parameter.getValue());
                    break;
                case "treatmentId":
                    computedBuilder.treatmentId(parameter.getValue());
                    break;
                case "offerName":
                    computedBuilder.offerName(parameter.getValue());
                    break;
                case "offerId":
                    computedBuilder.offerId(parameter.getValue());
                    break;
                case "usercaseTreatmentCode":
                    computedBuilder.usercaseTreatmentCode(parameter.getValue());
                    break;
                case "treatmentCode":
                    computedBuilder.treatmentCode(parameter.getValue());
                    break;
                case "customerType":
                    computedBuilder.customerType(parameter.getValue());
                    break;
                case "eventName":
                    computedBuilder.eventName(parameter.getValue());
                    break;
                case "mappingName":
                    computedBuilder.mappingName(parameter.getValue());
                    break;
                case "price":
                    computedBuilder.price(parameter.getValue());
                    break;
                case "offerCategory":
                    computedBuilder.offerCategory(parameter.getValue());
                    break;
                case "tgCgFlag":
                    computedBuilder.tgCgFlag(parameter.getValue());
                    break;
                case "direction":
                    computedBuilder.direction(parameter.getValue());
                    break;
                case "businessGroup":
                    computedBuilder.businessGroup(parameter.getValue());
                    break;
                case "businessSubGroup":
                    computedBuilder.businessSubGroup(parameter.getValue());
                    break;
                case "campaignObjective":
                    computedBuilder.campaignObjective(parameter.getValue());
                    break;
                case "campaignType":
                    computedBuilder.campaignType(parameter.getValue());
                    break;
                case "offerType":
                    computedBuilder.offerType(parameter.getValue());
                    break;
                case "offerStartDate":
                    computedBuilder.offerStartDate(parameter.getValue());
                    break;
                case "offerEndDate":
                    computedBuilder.offerEndDate(parameter.getValue());
                    break;
                case "altContactNumber":
                    computedBuilder.altContactNumber(parameter.getValue());
                    break;
                case "startHour":
                    computedBuilder.startHour(parameter.getValue());
                    break;
                case "endHour":
                    computedBuilder.endHour(parameter.getValue());
                    break;
                case "mainCampaignName":
                    computedBuilder.mainCampaignName(parameter.getValue());
                    break;
            }
        }

        computedBuilder.timestamp(now);

        return computedBuilder.build();
    }

}
