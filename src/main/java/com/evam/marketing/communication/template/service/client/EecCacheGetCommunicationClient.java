package com.evam.marketing.communication.template.service.client;

import com.evam.cache.model.CacheKey;
import com.evam.cache.model.CacheValue;
import com.evam.marketing.communication.template.cacheserver.CacheQueryService;
import com.evam.marketing.communication.template.cacheserver.ContactPolicyLimits;
import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.client.model.Parameter;
import com.evam.marketing.communication.template.service.event.KafkaProducerService;
import com.evam.marketing.communication.template.service.integration.HandlingService;
import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import jakarta.validation.constraints.NotNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EecCacheGetCommunicationClient extends AbstractCommunicationClient {

    private static final String PROVIDER = "SMS";
    private final HandlingService handlingService;
    private final CacheQueryService cacheQueryService;
    private static final Set<String> REQUIRED_KEYS = Set.of("testModeEnabled", "messageType", "applyContactPolicy");

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

            String applyContactPolicy = requiredFields.get("applyContactPolicy");
            if ("FALSE".equalsIgnoreCase(applyContactPolicy)) {
                req.setQuotaCheck("OK");
                handlingService.handleRequest(req);
                log.info("Quota check is not required for scenario '{}'. Skipping all limit checks.", campaignName);
                return generateSuccessCommunicationResponse(request, "Success", "Quota check not required.");
            }
            log.info("Quota check is required (applyContactPolicy={}). Proceeding with cache limit checks.", applyContactPolicy);

            if (!requiredFields.keySet().containsAll(Set.of("messageType", "applyContactPolicy"))) {
                String missingKeys = REQUIRED_KEYS.stream().filter(k -> !requiredFields.containsKey(k)).collect(Collectors.joining(", "));
                req.setQuotaCheck("MISSING_PARAMS");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Missing required parameters: " + missingKeys, "MISSING_PARAMS");
            }

            String messageType = requiredFields.get("messageType");
            CacheKey cacheKeyTrx = new CacheKey(actorId);
            ObjectMapper objectMapper = new ObjectMapper();

            List<Map<String, Object>> dailyTransactions = new ArrayList<>();
            List<Map<String, Object>> histTransactions = new ArrayList<>();
            try {
                CacheValue dailyValue = cacheQueryService.getTrxDailyCache().get(cacheKeyTrx);
                if (dailyValue != null && dailyValue.getValues().length > 0 && dailyValue.getValues()[0] != null) {
                    dailyTransactions = objectMapper.readValue(dailyValue.getValues()[0], new TypeReference<>() {});
                }
                CacheValue histValue = cacheQueryService.getTrxHistCache().get(cacheKeyTrx);
                if (histValue != null && histValue.getValues().length > 0 && histValue.getValues()[0] != null) {
                    histTransactions = objectMapper.readValue(histValue.getValues()[0], new TypeReference<>() {});
                }
            } catch (Exception e) {
                log.error("Fatal error reading transaction caches for actorId: {}. Aborting checks.", actorId, e);
                req.setQuotaCheck("CACHE_READ_ERROR");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Could not read transaction history.", "CACHE_READ_ERROR");
            }


            ContactPolicyLimits limitsAndUsage;

            Predicate<Map<String, Object>> allFilter = tx -> true;
            limitsAndUsage = cacheQueryService.getLimitsAndUsageForLevel(new CacheKey(actorId, "ALL", "ALL"), dailyTransactions, histTransactions, allFilter);
            if (limitsAndUsage == null) {
                req.setQuotaCheck("POLICY_CHECK_ERROR");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Error checking Actor-Level policies", "POLICY_CHECK_ERROR"); }

            if (limitsAndUsage.getDailyLimit() >= 0 && limitsAndUsage.getDailyCount() >= limitsAndUsage.getDailyLimit()) {
                req.setQuotaCheck("ACTOR_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Actor-Level Daily limit exceeded", "ACTOR_LIMIT_EXCEEDED");
            }
            if (limitsAndUsage.getWeeklyLimit() >= 0 && (limitsAndUsage.getDailyCount() + limitsAndUsage.getWeeklySum()) >= limitsAndUsage.getWeeklyLimit()) {
                req.setQuotaCheck("ACTOR_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Actor-Level Weekly limit exceeded", "ACTOR_LIMIT_EXCEEDED");
            }
            if (limitsAndUsage.getMonthlyLimit() >= 0 && (limitsAndUsage.getDailyCount() + limitsAndUsage.getMonthlySum()) >= limitsAndUsage.getMonthlyLimit()) {
                req.setQuotaCheck("ACTOR_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Actor-Level Monthly limit exceeded", "ACTOR_LIMIT_EXCEEDED");
            }

            Predicate<Map<String, Object>> smsFilter = tx -> "SMS".equals(String.valueOf(tx.get("CHANNEL")));
            limitsAndUsage = cacheQueryService.getLimitsAndUsageForLevel(new CacheKey(actorId, "SMS", "ALL"), dailyTransactions, histTransactions, smsFilter);
            if (limitsAndUsage == null) {
                req.setQuotaCheck("POLICY_CHECK_ERROR");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Error checking Channel-Level policies", "POLICY_CHECK_ERROR"); }

            if (limitsAndUsage.getDailyLimit() >= 0 && limitsAndUsage.getDailyCount() >= limitsAndUsage.getDailyLimit()) {
                req.setQuotaCheck("CHANNEL_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Channel-Level (SMS) Daily limit exceeded", "CHANNEL_LIMIT_EXCEEDED");
            }
            if (limitsAndUsage.getWeeklyLimit() >= 0 && (limitsAndUsage.getDailyCount() + limitsAndUsage.getWeeklySum()) >= limitsAndUsage.getWeeklyLimit()) {
                req.setQuotaCheck("CHANNEL_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Channel-Level (SMS) Weekly limit exceeded", "CHANNEL_LIMIT_EXCEEDED");
            }
            if (limitsAndUsage.getMonthlyLimit() >= 0 && (limitsAndUsage.getDailyCount() + limitsAndUsage.getMonthlySum()) >= limitsAndUsage.getMonthlyLimit()) {
                req.setQuotaCheck("CHANNEL_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Channel-Level (SMS) Monthly limit exceeded", "CHANNEL_LIMIT_EXCEEDED");
            }

            Predicate<Map<String, Object>> specificFilter = tx -> "SMS".equals(String.valueOf(tx.get("CHANNEL"))) && messageType.equals(String.valueOf(tx.get("MESSAGE_TYPE")));
            ContactPolicyLimits specificLevelLimits = cacheQueryService.getLimitsAndUsageForLevel(new CacheKey(actorId, "SMS", messageType), dailyTransactions, histTransactions, specificFilter);
            if (specificLevelLimits == null) {
                req.setQuotaCheck("POLICY_CHECK_ERROR");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Error checking MessageType-Level policies", "POLICY_CHECK_ERROR"); }

            if (specificLevelLimits.getDailyLimit() >= 0 && specificLevelLimits.getDailyCount() >= specificLevelLimits.getDailyLimit()) {
                req.setQuotaCheck("MESSAGETYPE_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "MessageType-Level Daily limit exceeded", "MESSAGETYPE_LIMIT_EXCEEDED");
            }
            if (specificLevelLimits.getWeeklyLimit() >= 0 && (specificLevelLimits.getDailyCount() + specificLevelLimits.getWeeklySum()) >= specificLevelLimits.getWeeklyLimit()) {
                req.setQuotaCheck("MESSAGETYPE_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "MessageType-Level Weekly limit exceeded", "MESSAGETYPE_LIMIT_EXCEEDED");
            }
            if (specificLevelLimits.getMonthlyLimit() >= 0 && (specificLevelLimits.getDailyCount() + specificLevelLimits.getMonthlySum()) >= specificLevelLimits.getMonthlyLimit()) {
                req.setQuotaCheck("MESSAGETYPE_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "MessageType-Level Monthly limit exceeded", "MESSAGETYPE_LIMIT_EXCEEDED");
            }

            req.setQuotaCheck("OK");
            handlingService.handleRequest(req);

            long updatedDailySum = specificLevelLimits.getDailyCount() + 1;
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