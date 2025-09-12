package com.evam.marketing.communication.template.service.client;

import com.evam.marketing.communication.template.cacheserver.CacheQueryService;
import com.evam.marketing.communication.template.cacheserver.ContactPolicyLimits;
import com.evam.marketing.communication.template.service.client.model.*;
import com.evam.marketing.communication.template.service.event.KafkaProducerService;
import com.evam.marketing.communication.template.service.event.model.CommunicationResponseEvent;
import com.evam.marketing.communication.template.service.integration.HandlingService;
import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EecCacheGetCommunicationClient extends AbstractCommunicationClient {

    private static final String PROVIDER = "SMS";
    private final HandlingService handlingService;
    private final CacheQueryService cacheQueryService;
    private static final Set<String> REQUIRED_KEYS = Set.of("testModeEnabled","messageType", "applyContactPolicy");

    @Autowired
    public EecCacheGetCommunicationClient(KafkaProducerService kafkaProducerService,
                                          HandlingService handlingService, CacheQueryService cacheQueryService) {
        super(kafkaProducerService);
        this.cacheQueryService = cacheQueryService;
        this.handlingService = handlingService;
    }

    private Map<String, String> extractRequiredFields(CustomCommunicationRequest req) {
        Map<String, String> out = new HashMap<>();
        if (req.getParameters() == null) return out;
        for (Parameter p : req.getParameters()) {
            if (p != null && StringUtils.isNotBlank(p.getName()) && REQUIRED_KEYS.contains(p.getName()) && StringUtils.isNotBlank(p.getValue())) {
                out.put(p.getName(), p.getValue());
            }
        }
        return out;
    }

    @RateLimiter(name = "client-limiter")
    @NotNull
    @Override
    public CommunicationResponse send(CommunicationRequest request) {
        CustomCommunicationRequest req = (CustomCommunicationRequest) request;
        String actorId = req.getActorId();
        String campaignName = req.getScenario();

        try {
            log.debug("Checking quota requirement for scenario '{}'", campaignName);
            Map<String, String> requiredFields = extractRequiredFields(req);
//            if(Boolean.parseBoolean(requiredFields.get("testModeEnabled"))){
//                req.setQuotaCheck("OK");
//                handlingService.handleRequest(req);
//                CommunicationResponse ok = generateSuccessCommunicationResponse(request, "Success", "Test mode enabled.No quota check performed.");
//                return ok;
//            }
            String applyContactPolicy = requiredFields.get("applyContactPolicy");
            if ("FALSE".equalsIgnoreCase(applyContactPolicy)) {
//                log.info("Quota check is not required for scenario '{}'. Skipping all limit checks.", campaignName);
//                CommunicationResponse ok = generateSuccessCommunicationResponse(request, "Success", "Quota check not required.");
//                CommunicationResponseEvent evt = ok.toEvent();
//                evt.addCustomParameter("quotaCheckSkipped", "true");
//                sendEvent(evt);
                //İŞLEMİ YAP
                req.setQuotaCheck("OK");
                handlingService.handleRequest(req);
                log.info("Quota check is not required for scenario '{}'. Skipping all limit checks.", campaignName);
                CommunicationResponse ok = generateSuccessCommunicationResponse(request, "Success", "Quota check not required.");
                return ok;
            }
            log.info("Quota check is required (applyContactPolicy={}). Proceeding with cache limit checks.", applyContactPolicy);

            //Map<String, String> requiredFields = extractRequiredFields(req);
            if (!requiredFields.keySet().containsAll(REQUIRED_KEYS)) {
                String missingKeys = REQUIRED_KEYS.stream().filter(k -> !requiredFields.containsKey(k)).collect(Collectors.joining(", "));
                return generateFailCommunicationResponse(request, "Missing required parameters: " + missingKeys, "MISSING_PARAMS");
            }

            String channel = "SMS";
            String messageType = requiredFields.get("messageType");
            log.info("messageType : {}",messageType);
            String cacheKey = String.join(",", actorId, channel, messageType);
            log.info("Constructed cacheKey: {}", cacheKey);

            // --- CACHE'TEN VERİ ÇEKME İŞLEMİ ---
            ContactPolicyLimits contactPolicyLimits = cacheQueryService.getCacheValues(req,requiredFields);

            if(contactPolicyLimits == null){
                String msg = "Failed to retrieve contact policy limits from cache for key: " + cacheKey;
                log.error(msg);
                req.setQuotaCheck("CONTACT_POLICY");
                handlingService.handleRequest(req);
                CommunicationResponse fail = generateFailCommunicationResponse(request, msg, "CONTACT_POLICY_NOT_FOUND");
                return fail;
            }

            if (contactPolicyLimits.getDailyLimit() >= 0 && contactPolicyLimits.getDailyCount() >= contactPolicyLimits.getDailyLimit()) {
                String msg = String.format("Daily limit exceeded. Limit: %d, Current Sent: %d", contactPolicyLimits.getDailyLimit(), contactPolicyLimits.getDailyCount());
                req.setQuotaCheck("DAILY_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                CommunicationResponse fail = generateFailCommunicationResponse(request, msg, "DAILY_LIMIT_EXCEEDED");
                return fail;
            }

            long currentWeeklyTotal = contactPolicyLimits.getDailyCount() + contactPolicyLimits.getWeeklySum();
            if (contactPolicyLimits.getWeeklyLimit() >= 0 && currentWeeklyTotal >= contactPolicyLimits.getWeeklyLimit()) {
                String msg = String.format("Weekly limit exceeded. Limit: %d, Current Total (sentToday + weeklySum): %d", contactPolicyLimits.getWeeklyLimit(), currentWeeklyTotal);
                CommunicationResponse fail = generateFailCommunicationResponse(request, msg, "WEEKLY_LIMIT_EXCEEDED");
                req.setQuotaCheck("WEEKLY_LIMIT_EXCEEDED");
                return fail;
            }

            long currentMonthlyTotal = contactPolicyLimits.getDailyCount() + contactPolicyLimits.getMonthlySum();
            if (contactPolicyLimits.getMonthlyLimit() >= 0 && currentMonthlyTotal >= contactPolicyLimits.getMonthlyLimit()) {
                String msg = String.format("Monthly limit exceeded. Limit: %d, Current Total (sentToday + monthlySum): %d", contactPolicyLimits.getMonthlyLimit(), currentMonthlyTotal);
                CommunicationResponse fail = generateFailCommunicationResponse(request, msg, "MONTHLY_LIMIT_EXCEEDED");
                req.setQuotaCheck("MONTHLY_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return fail;
            }

            //İŞLEMİ YAP
            req.setQuotaCheck("OK");
            handlingService.handleRequest(req);
            long updatedDailySum = contactPolicyLimits.getDailyCount() + 1;
            try {
                cacheQueryService.putCacheValue(req, requiredFields, String.valueOf(updatedDailySum));
            } catch (Exception e) {
                log.error("Failed to update cache after successful quota check for actorId: {}. Error: {}", actorId, e.getMessage());
            }
            CommunicationResponse ok = generateSuccessCommunicationResponse(request, "Success", "All limits checked and valid. Daily sent count updated.");
            return ok;

        } catch (Exception e) {
            log.error("Unexpected error while fetching or checking cache data. RequestId={} request={}", req.getCommunicationUUID(), request, e);
            CommunicationResponse fail = generateFailCommunicationResponse(request, e.getMessage(), "UNEXPECTED_ERROR");
            req.setQuotaCheck("UNEXPECTED_ERROR");
            handlingService.handleRequest(req);
            return fail;
        }
    }

    @Override
    public String getProvider() {
        return PROVIDER;
    }
}