package com.evam.marketing.communication.template.service.integration;

import com.evam.marketing.communication.template.configuration.PersistentConfig;
import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.client.model.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.time.ZonedDateTime;
import java.util.*;

@Service
@Slf4j
public class HandlingService {
    private final PersistentConfig persistentConfig;
    private final PersistenceService persistenceService;

    // 1. Test aktörleri için Set tanımlayın
    private final Set<String> testActorsSet;
    private final String CONTROL_GROUP = "CONTROL_GROUP";

    public HandlingService(PersistentConfig persistentConfig, PersistenceService persistenceService) {
        this.persistentConfig = persistentConfig;
        this.persistenceService = persistenceService;

        if (persistentConfig.getTestActors() != null && !persistentConfig.getTestActors().isEmpty()) {
            this.testActorsSet = new HashSet<>(Arrays.asList(persistentConfig.getTestActors().split(",")));
            log.debug("Test actors loaded from config: {}", this.testActorsSet);
        } else {
            this.testActorsSet = Collections.emptySet();
            log.debug("No test actors found in config.");
        }
    }


public void handleRequest(CustomCommunicationRequest customCommunicationRequest) {

    try {
        ZonedDateTime now = ZonedDateTime.now();

        ServiceRequest serviceRequest = getServiceRequest(now, customCommunicationRequest);
        String actorId = customCommunicationRequest.getActorId();
        // ANA KONTROL: Sistem "Test Modu"nda mı, yoksa "Canlı Mod"da mı?
        if (serviceRequest.getComputed().isTestModeEnabled()) {
            // --- SİSTEM TEST MODUNDA ---
            // Sadece bu moddayken test aktörü kontrolü yapılır.
            boolean isTestActor = testActorsSet.contains(actorId);

            if (isTestActor) {
                // Test modunda olmamıza rağmen, bu özel aktöre GERÇEK push gönderilecek.
                serviceRequest.getComputed().setTestModeEnabled(false);
                log.info("SYSTEM IN TEST MODE - Test Actor {} detected. Overriding to LIVE for real push.", actorId);
            } else {
                // Test modundayız ve bu normal bir kullanıcı, push gönderilmeyecek.
                log.debug("SYSTEM IN TEST MODE - Regular Actor {} detected. Stays in test mode.", actorId);
            }
        } else {
            // --- SİSTEM CANLI MODDA ---
            // Test aktörü kontrolü YAPILMAZ. Tüm istekler canlı olarak işlenir.
            // Canlı modda logları şişirmemek için DEBUG seviyesinde logluyoruz.
            log.debug("SYSTEM IN LIVE MODE - Processing actor {}.", actorId);
        }
        persistenceService.enqueue(serviceRequest);
//

//        if (testActorsSet.contains(customCommunicationRequest.getActorId())) {
//            serviceRequest.getComputed().setTestModeEnabled(false);
//            log.debug("Test actor detected. ActorId: {}", customCommunicationRequest.getActorId());
//            log.debug("Test mode : {}", serviceRequest.getComputed().isTestModeEnabled());
//        }
//
//        log.debug("Actor id {}", customCommunicationRequest.getActorId());
//        persistenceService.enqueue(serviceRequest);

        //Eski test Actor yapısı
//            List<String> testActors = Arrays.asList(persistentConfig.getTestActors().split(","));
//            log.debug("Test actors from config: {}", testActors);
//            if(testActors.contains(customCommunicationRequest.getActorId())){
//                serviceRequest.getComputed().setTestModeEnabled(false);
//                log.debug("Test actor detected. ActorId: {}", customCommunicationRequest.getActorId());
//                log.debug("Test mode : {}",serviceRequest.getComputed().isTestModeEnabled());
//            }
//            log.debug("Actor id {}", customCommunicationRequest.getActorId());
//            persistenceService.enqueue(serviceRequest);
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

