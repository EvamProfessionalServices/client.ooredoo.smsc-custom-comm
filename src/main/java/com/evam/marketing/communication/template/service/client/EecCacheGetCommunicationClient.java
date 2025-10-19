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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
@Slf4j
public class EecCacheGetCommunicationClient extends AbstractCommunicationClient {

    /**
     * Kural verilerini, önceliğini ve ilgili filtresini bir arada tutmak için yardımcı bir iç sınıf (inner class).
     */
    @Getter
    @AllArgsConstructor
    private static class PolicyRule {
        private int priority;
        private List<String> limits;
        private Predicate<Map<String, Object>> usageFilter;
        private String policyName;
    }

    private static final String PROVIDER = "SMS";
    private final HandlingService handlingService;
    private final CacheQueryService cacheQueryService;
    private static final Set<String> REQUIRED_KEYS = Set.of("testModeEnabled", "messageType", "applyContactPolicy","properStartHour","properEndHour");
    private static final ObjectMapper objectMapper = new ObjectMapper();

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

    private String formatLimitLog(ContactPolicyLimits limits, String failReason) {
        long dailyCount = limits.getDailyCount();
        long dailyLimit = limits.getDailyLimit();
        long weeklyTotal = dailyCount + limits.getWeeklySum();
        long weeklyLimit = limits.getWeeklyLimit();
        long monthlyTotal = dailyCount + limits.getMonthlySum();
        long monthlyLimit = limits.getMonthlyLimit();

        String dailyLog = "DAILY".equals(failReason)
                ? String.format("<%d/%d>", dailyCount, dailyLimit)
                : String.format("%d/%d", dailyCount, dailyLimit);

        String weeklyLog = "WEEKLY".equals(failReason)
                ? String.format("<%d/%d>", weeklyTotal, weeklyLimit)
                : String.format("%d/%d", weeklyTotal, weeklyLimit);

        String monthlyLog = "MONTHLY".equals(failReason)
                ? String.format("<%d/%d>", monthlyTotal, monthlyLimit)
                : String.format("%d/%d", monthlyTotal, monthlyLimit);

        return String.format("[daily: %s, weekly: %s, monthly: %s]", dailyLog, weeklyLog, monthlyLog);
    }

    @RateLimiter(name = "client-limiter")
    @NotNull
    @Override
    public CommunicationResponse send(CommunicationRequest request) {
        CustomCommunicationRequest req = (CustomCommunicationRequest) request;
        String actorId = req.getActorId();
        try {
            Map<String, String> requiredFields = extractRequiredFields(req);
            try {
                if (!isNowWithin(requiredFields.get("properStartHour"), requiredFields.get("properEndHour")))
                {
                    req.setQuotaCheck("PROPER_TIME");
                    handlingService.handleRequest(req);
                    log.debug("Current time is out of proper time range for scenario '{}'. Skipping all limit checks.", req.getScenario());
                    return generateFailCommunicationResponse(request, "Current time is out of proper time range.", "PROPER_TIME");
                }
            } catch (Exception e) {
                req.setQuotaCheck("PROPER_TIME");
                handlingService.handleRequest(req);
                log.error("Error checking proper time range for actorId {}. Proceeding with limit checks. Error: {}", actorId, e.getMessage());
                return generateFailCommunicationResponse(request, "Current time is out of proper time range.", "PROPER_TIME");
            }

            String applyContactPolicy = requiredFields.get("applyContactPolicy");
            if ("FALSE".equalsIgnoreCase(applyContactPolicy)) {
                req.setQuotaCheck("OK");
                handlingService.handleRequest(req);
                log.debug("Quota check is not required for scenario '{}'. Skipping all limit checks.", req.getScenario());
                return generateSuccessCommunicationResponse(request, "Success", "Quota check not required.");
            }
            log.debug("Quota check is required (applyContactPolicy={}). Proceeding with cache limit checks.", applyContactPolicy);

            if (!requiredFields.containsKey("messageType")) {
                req.setQuotaCheck("MISSING_PARAMS");
                handlingService.handleRequest(req);
                return generateFailCommunicationResponse(request, "Missing required parameter: messageType", "MISSING_PARAMS");
            }

            String messageType = requiredFields.get("messageType");
            String channel = "SMS";
//            ObjectMapper objectMapper = new ObjectMapper();

            // 1. Transaction verilerini SADECE BİR KEZ oku
            List<Map<String, Object>> dailyTransactions = new ArrayList<>();
            List<Map<String, Object>> histTransactions = new ArrayList<>();
            try {
                CacheKey cacheKeyTrx = new CacheKey(actorId);
                CacheValue dailyValue = cacheQueryService.getTrxDailyCache().get(cacheKeyTrx);
                if (dailyValue != null && dailyValue.getValues().length > 0 && dailyValue.getValues()[0] != null) {
                    dailyTransactions = objectMapper.readValue(dailyValue.getValues()[0], new TypeReference<>() {
                    });
                }
                CacheValue histValue = cacheQueryService.getTrxHistCache().get(cacheKeyTrx);
                if (histValue != null && histValue.getValues().length > 0 && histValue.getValues()[0] != null) {
                    histTransactions = objectMapper.readValue(histValue.getValues()[0], new TypeReference<>() {
                    });
                }
            } catch (Exception e) {
                req.setQuotaCheck("CACHE_READ_ERROR");
                handlingService.handleRequest(req);
                log.error("Fatal error reading transaction caches for actorId: {}. Aborting checks.", actorId, e);
                return generateFailCommunicationResponse(request, "Could not read transaction history.", "CACHE_READ_ERROR");
            }

            //ACTOR SEGMENT TABLOSUNDAN SEGMENT BILGISI ÇEK
            String segmentName = cacheQueryService.getSegmentNameByActorId(req.getActorId());
            log.debug("Segment name for actorId {}: {}", actorId, segmentName);
            if(segmentName==null){
                req.setQuotaCheck("CONTACT_POLICY_NOT_FOUND");
                handlingService.handleRequest(req);
                log.error("No contact policy found for actorId {} after trying all 4 rule levels.", actorId);

                return generateFailCommunicationResponse(request, "Could not found actor - contact policy segment relation.", "CONTACT_POLICY_NOT_FOUND");
            }

            // 2. Dört potansiyel kural anahtarını ve filtrelerini tanımla
            Map<CacheKey, Predicate<Map<String, Object>>> potentialRules = new LinkedHashMap<>();
            potentialRules.put(new CacheKey(segmentName, channel, messageType), tx -> channel.equals(String.valueOf(tx.get("CHANNEL"))) && messageType.equals(String.valueOf(tx.get("MESSAGE_TYPE"))));
            potentialRules.put(new CacheKey(segmentName, "ALL", messageType), tx -> messageType.equals(String.valueOf(tx.get("MESSAGE_TYPE"))));


            // 3. Tüm potansiyel kuralları cache'ten çek
            List<PolicyRule> foundPolicies = new ArrayList<>();
            for (Map.Entry<CacheKey, Predicate<Map<String, Object>>> entry : potentialRules.entrySet()) {
                log.debug("Checking policy for key: {}", entry.getKey());
                List<String> values = cacheQueryService.getPolicyValues(entry.getKey());
                if (values != null && !values.isEmpty()) {
                    // Varsayım: 0. indeks PRIORITY
                    int priority = Integer.parseInt(values.get(0));
                    foundPolicies.add(new PolicyRule(priority, values, entry.getValue(), entry.getKey().toString()));
                }
            }

            // 4. En az bir kural bulunması zorunlu. Bulunamazsa hata dön.
            if (foundPolicies.isEmpty()) {
                req.setQuotaCheck("CONTACT_POLICY_NOT_FOUND");
                handlingService.handleRequest(req);
                log.error("No contact policy found for actorId {} after trying all 4 rule levels.", actorId);
                return generateFailCommunicationResponse(request, "No contact policy defined for actor.", "CONTACT_POLICY_NOT_FOUND");
            }

            // 5. En düşük öncelikli kuralı seç
            PolicyRule activePolicyRule = Collections.min(foundPolicies, Comparator.comparingInt(PolicyRule::getPriority));
            log.debug("Active policy for actorId {}: {} with priority {}", actorId, activePolicyRule.getPolicyName(), activePolicyRule.getPriority());




            // Sadece mevcut işleme ait channel ve messageType'a özel bir filtre oluştur.
            Predicate<Map<String, Object>> specificFilter = tx -> channel.equals(String.valueOf(tx.get("CHANNEL"))) &&
                    messageType.equals(String.valueOf(tx.get("MESSAGE_TYPE")));

            // Yeni metodu kullanarak değerleri doğrudan oku.
            int dailyCount = cacheQueryService.getSpecificTransactionValue(dailyTransactions, specificFilter, "SENT_COUNT");
            int weeklySum = cacheQueryService.getSpecificTransactionValue(histTransactions, specificFilter, "WEEKLY_SUM");
            int monthlySum = cacheQueryService.getSpecificTransactionValue(histTransactions, specificFilter, "MONTHLY_SUM");


            // Varsayım: 1, 2, 3. indeksler limitlerdir
            int dailyLimit = Integer.parseInt(activePolicyRule.getLimits().get(1));
            int weeklyLimit = Integer.parseInt(activePolicyRule.getLimits().get(2));
            int monthlyLimit = Integer.parseInt(activePolicyRule.getLimits().get(3));

            ContactPolicyLimits limitsToApply = new ContactPolicyLimits(dailyLimit, weeklyLimit, monthlyLimit, dailyCount, weeklySum, monthlySum);

            // 7. Limiti kontrol et
            if (limitsToApply.getDailyLimit() >= 0 && limitsToApply.getDailyCount() >= limitsToApply.getDailyLimit()) {
                req.setQuotaCheck("DAILY_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                String logDetails = formatLimitLog(limitsToApply, "DAILY");
                log.debug("LIMIT EXCEEDED for actorId {}. Policy: {}. Details: {}", actorId, activePolicyRule.getPolicyName(), logDetails);
                return generateFailCommunicationResponse(request, activePolicyRule.getPolicyName() + " Daily limit exceeded", "DAILY_LIMIT_EXCEEDED");
            }
            if (limitsToApply.getWeeklyLimit() >= 0 && (limitsToApply.getDailyCount() + limitsToApply.getWeeklySum()) >= limitsToApply.getWeeklyLimit()) {
                req.setQuotaCheck("WEEKLY_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                String logDetails = formatLimitLog(limitsToApply, "WEEKLY");
                log.debug("LIMIT EXCEEDED for actorId {}. Policy: {}. Details: {}", actorId, activePolicyRule.getPolicyName(), logDetails);
                return generateFailCommunicationResponse(request, activePolicyRule.getPolicyName() + " Weekly limit exceeded", "WEEKLY_LIMIT_EXCEEDED");
            }
            if (limitsToApply.getMonthlyLimit() >= 0 && (limitsToApply.getDailyCount() + limitsToApply.getMonthlySum()) >= limitsToApply.getMonthlyLimit()) {
                req.setQuotaCheck("MONTHLY_LIMIT_EXCEEDED");
                handlingService.handleRequest(req);
                String logDetails = formatLimitLog(limitsToApply, "MONTHLY");
                log.debug("LIMIT EXCEEDED for actorId {}. Policy: {}. Details: {}", actorId, activePolicyRule.getPolicyName(), logDetails);
                return generateFailCommunicationResponse(request, activePolicyRule.getPolicyName() + " Monthly limit exceeded", "MONTHLY_LIMIT_EXCEEDED");
            }

            // KONTROLDEN GEÇTİ
            String logDetails = formatLimitLog(limitsToApply, "OK");
            log.debug("LIMIT CHECK PASSED for actorId {}. Policy: {}. Details: {}", actorId, activePolicyRule.getPolicyName(), logDetails);

            //İŞLEMİ YAP
            req.setQuotaCheck("OK");
            handlingService.handleRequest(req);

            // Cache'i güncellerken HER ZAMAN gönderilen spesifik mesaj tipinin sayısını hesapla ve 1 artır.
            Predicate<Map<String, Object>> specificFilterForUpdate = tx -> channel.equals(String.valueOf(tx.get("CHANNEL"))) && messageType.equals(String.valueOf(tx.get("MESSAGE_TYPE")));
            int specificDailyCount = cacheQueryService.sumTransactionValues(dailyTransactions, specificFilterForUpdate, "SENT_COUNT");
            long updatedDailySum = specificDailyCount + 1;

            try {
                cacheQueryService.putCacheValue(req, requiredFields, String.valueOf(updatedDailySum));
                log.debug("Cache put for actor {}",request.getActorId());
            } catch (Exception e) {

                log.error("Failed to update cache after successful quota check for actorId: {}. Error: {}", actorId, e.getMessage());
            }
            return generateSuccessCommunicationResponse(request, "Success", "All limits checked and valid. Daily sent count updated.");

        } catch (Exception e) {
            log.error("Unexpected error while fetching or checking cache data. RequestId={} request={}", req.getCommunicationUUID(), request, e);
            CommunicationResponse fail = generateFailCommunicationResponse(request, e.getMessage(), "UNEXPECTED_ERROR");
            req.setQuotaCheck("UNEXPECTED_ERROR");
            handlingService.handleRequest(req);
            return fail;
        }
    }
    public boolean isNowWithin(String startHHmm, String endHHmm) {
        try {
            LocalTime start = LocalTime.parse(startHHmm);
            LocalTime end = LocalTime.parse(endHHmm);
            LocalTime now = LocalTime.now();

            // ÖZEL DURUM: 00:00 - 00:00 => her zaman true
            if (start.equals(LocalTime.MIDNIGHT) && end.equals(LocalTime.MIDNIGHT)) {
                return true;
            }

            // Normal durum (inclusive değil)
            if (now.isAfter(start) && now.isBefore(end)) {
                return true;
            }

        } catch (Exception e) {
            log.error("Error parsing proper time range: start='{}', end='{}'. Error: {}", startHHmm, endHHmm, e.getMessage());
            return false;
        }

        return false;
    }




    @Override
    public String getProvider() {
        return PROVIDER;
    }
}