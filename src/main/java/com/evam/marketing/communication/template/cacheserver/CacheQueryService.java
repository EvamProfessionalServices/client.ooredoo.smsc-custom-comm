package com.evam.marketing.communication.template.cacheserver;

import com.evam.cache.client.CacheContainer;
import com.evam.cache.client.CacheValueDescriptor;
import com.evam.cache.client.WaitingCacheContainer;
import com.evam.cache.common.CustomTcpDiscoverySpi;
import com.evam.cache.common.IgniteConfigurationReader;
import com.evam.cache.model.CacheKey;
import com.evam.cache.model.CacheValue;
import com.evam.marketing.communication.template.cacheserver.DTO.CustomerDetails;
import com.evam.marketing.communication.template.cacheserver.DTO.ScenarioMetaParams;
import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.client.model.Parameter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.LinkedHashMap;


import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;


@Service
@Slf4j
@Getter
public class CacheQueryService {
    private Ignite ignite;
    private IgniteCache<CacheKey, CacheValue> contactPolicyCache;
    private CacheValueDescriptor contactPolicyCacheValueDescriptor;
    private IgniteCache<CacheKey, CacheValue> trxDailyCache;
    private CacheValueDescriptor trxDailyCacheValueDescriptor;
    private IgniteCache<CacheKey, CacheValue> trxHistCache;
    private CacheValueDescriptor trxHistCacheValueDescriptor;
    private IgniteCache<CacheKey, CacheValue> customerDetailsCache;
    private CacheValueDescriptor customerDetailsCacheValueDescriptor;
    private IgniteCache<CacheKey, CacheValue> scenarioMetaParamsCache;
    private CacheValueDescriptor scenarioMetaParamsCacheValueDescriptor;

    @Value("${eec.pool-cache-name}")
    private String contactPolicyCacheName;
    @Value("${eec.trx-daily-cache}")
    private String trxDailyCacheName;
    @Value("${eec.trx-hist-cache}")
    private String trxHistCacheName;
    @Value("${eec.customer-detail-cache}")
    private String customerDetailsCacheName;
    @Value("${eec.scenario-meta-params-cache}")
    private String scenarioMetaParamsCacheName;

    @Value("${eec.ignite-client-file-path}")   private String igniteClientFilePath;
    @Value("${eec.app-token}")  private String eecAppToken;


    private int sumTransactionValues(List<Map<String, Object>> transactions, Predicate<Map<String, Object>> filter, String key) {
        if (transactions == null || transactions.isEmpty()) {
            return 0;
        }
        return transactions.stream()
                .filter(filter)
                .mapToInt(tx -> tx.containsKey(key) ? ((Number) tx.get(key)).intValue() : 0)
                .sum();
    }

    public ContactPolicyLimits getLimitsAndUsageForLevel(CacheKey limitsKey,
                                                         List<Map<String, Object>> dailyTransactions,
                                                         List<Map<String, Object>> histTransactions,
                                                         Predicate<Map<String, Object>> filter) {
        try {
            CacheValue limitValue = contactPolicyCache.get(limitsKey);
            if (limitValue == null) {
                return new ContactPolicyLimits(0, 0, 0, 0, 0, 0);
            }

            List<String> limits = Arrays.asList(limitValue.getValues());
            int dailyLimit = Integer.parseInt(limits.get(1));
            int weeklyLimit = Integer.parseInt(limits.get(2));
            int monthlyLimit = Integer.parseInt(limits.get(3));

            int dailyCount = sumTransactionValues(dailyTransactions, filter, "SENT_COUNT");
            int weeklySum = sumTransactionValues(histTransactions, filter, "WEEKLY_SUM");
            int monthlySum = sumTransactionValues(histTransactions, filter, "MONTHLY_SUM");

            return new ContactPolicyLimits(dailyLimit, weeklyLimit, monthlyLimit, dailyCount, weeklySum, monthlySum);

        } catch (Exception e) {
            log.error("Error getting limits and usage for key: {}", limitsKey, e);
            return null; // Kritik bir hata durumunda null dön.
        }
    }

/*    public ContactPolicyLimits getCacheValues(CustomCommunicationRequest req, Map<String, String> requiredFields) {
        CacheKey cacheKey = new CacheKey(req.getActorId(), "SMS", requiredFields.get("messageType"));
        List<String> limits = Collections.emptyList();
        CacheKey cacheKeyTrx = new CacheKey(req.getActorId());

        int dailyCount = 0;
        int weeklySum = 0;
        int monthlySum = 0;

        try {
            limits = Arrays.asList(contactPolicyCache.get(cacheKey).getValues());
        } catch (Exception e) {
            log.error("Cache read error for limits key: {}", cacheKey, e);
            return null;
        }

        try {
            CacheValue dailyCacheValue = trxDailyCache.get(cacheKeyTrx);

            if (dailyCacheValue != null && dailyCacheValue.getValues().length > 0) {
                String dailyTransactionsJson = dailyCacheValue.getValues()[0];
                // JSON boş bir string değilse devam et.
                if (dailyTransactionsJson != null && !dailyTransactionsJson.isEmpty()) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode rootNode = objectMapper.readTree(dailyTransactionsJson);

                    if (rootNode.isArray()) {
                        String requiredMessageType = requiredFields.get("messageType");
                        for (JsonNode node : rootNode) {
                            String channel = node.path("CHANNEL").asText();
                            String messageType = node.path("MESSAGE_TYPE").asText();

                            if ("SMS".equals(channel) && requiredMessageType.equals(messageType)) {
                                dailyCount = node.path("SENT_COUNT").asInt();
                                break;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Cache read or JSON parse error for dailyTransactions key: {}", cacheKeyTrx, e);
        }

        try {
            CacheValue histCacheValue = trxHistCache.get(cacheKeyTrx);

            if (histCacheValue != null && histCacheValue.getValues().length > 0) {
                String histTransactionsJson = histCacheValue.getValues()[0];
                if (histTransactionsJson != null && !histTransactionsJson.isEmpty()) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode rootNode = objectMapper.readTree(histTransactionsJson);

                    if (rootNode.isArray()) {
                        String requiredMessageType = requiredFields.get("messageType");
                        for (JsonNode node : rootNode) {
                            String channel = node.path("CHANNEL").asText();
                            String messageType = node.path("MESSAGE_TYPE").asText();

                            if ("SMS".equals(channel) && requiredMessageType.equals(messageType)) {
                                weeklySum = node.path("WEEKLY_SUM").asInt();
                                monthlySum = node.path("MONTHLY_SUM").asInt();
                                break;
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Cache read or JSON parse error for histTransactions key: {}", cacheKeyTrx, e);
        }

        try {
            ContactPolicyLimits contactPolicyLimits = new ContactPolicyLimits(
                    Integer.valueOf(limits.get(1)),
                    Integer.valueOf(limits.get(2)),
                    Integer.valueOf(limits.get(3)),
                    dailyCount,
                    weeklySum,
                    monthlySum);
            return contactPolicyLimits;
        } catch (Exception e) {
            log.error("Conversion error for contactPolicyLimits key: {}", cacheKey, e);
            return null;
        }
    }

 */

    public void putCacheValue(CustomCommunicationRequest req, Map<String, String> requiredFields, String updatedDailySum) {
        try {
            CacheKey cacheKey = new CacheKey(req.getActorId());
            ObjectMapper objectMapper = new ObjectMapper();

            CacheValue existingValue = trxDailyCache.get(cacheKey);
            List<Map<String, Object>> transactionsList;

            if (existingValue == null || existingValue.getValues()[0].isEmpty()) {
                transactionsList = new ArrayList<>();
            } else {
                String jsonString = existingValue.getValues()[0];
                transactionsList = objectMapper.readValue(jsonString, new TypeReference<List<Map<String, Object>>>() {});
            }

            String requiredMessageType = requiredFields.get("messageType");
            boolean entryFoundAndUpdated = false;

            for (Map<String, Object> transaction : transactionsList) {
                String channel = (String) transaction.get("CHANNEL");
                String messageType = (String) transaction.get("MESSAGE_TYPE");

                if ("SMS".equals(channel) && requiredMessageType.equals(messageType)) {
                    transaction.put("SENT_COUNT", Integer.valueOf(updatedDailySum));
                    entryFoundAndUpdated = true;
                    break;
                }
            }

            if (!entryFoundAndUpdated) {
                Map<String, Object> newTransaction = new LinkedHashMap<>();

                newTransaction.put("CHANNEL", "SMS");
                newTransaction.put("MESSAGE_TYPE", requiredMessageType);
                newTransaction.put("SENT_COUNT", Integer.valueOf(updatedDailySum));

                transactionsList.add(newTransaction);
            }

            String updatedJson = objectMapper.writeValueAsString(transactionsList);

            CacheValue newCacheValue = new CacheValue(new String[]{updatedJson}, 1, 1);
            trxDailyCache.put(cacheKey, newCacheValue);

        } catch (Exception e) {
            log.error("Cache write error during read-modify-write for dailyTransactions key: {}", req.getActorId(), e);
        }
    }


    private Ignite getIgnite() throws IOException {
        do {
            IgniteConfiguration igniteConfiguration = getIgniteConfiguration();
            try {
                ignite = Ignition.getOrStart(igniteConfiguration);
                /*
                 * WARNING!!! do not export to variable in order not to instantiate once
                 * of igniteConfiguration. Ignore CAST Errors. Because it prevents error
                 * "SPI has already been started (always create new configuration instance
                 * for each starting Ignite instances)"
                 */
                break;
            } catch (IgniteException e) {
                log.error("EEC connection can not be established. Since EEC connection configured " +
                                "try until connected for each {} ms. message {}",
                        ((CustomTcpDiscoverySpi) igniteConfiguration.getDiscoverySpi()).getJoinTimeout(),
                        e.getMessage());
                log.error("", e);
                Ignition.stop(igniteConfiguration.getGridName(), true);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        } while (!Thread.currentThread().isInterrupted());

        return ignite;
    }

    @NotNull
    private IgniteConfiguration getIgniteConfiguration() throws IOException {
        try (FileInputStream igniteConf = new FileInputStream(igniteClientFilePath)) {
            IgniteConfiguration configuration;
            configuration = new IgniteConfigurationReader().readConfiguration(igniteConf);
            CustomTcpDiscoverySpi discoverySpi = (CustomTcpDiscoverySpi) configuration.getDiscoverySpi();
            discoverySpi.setApplicationToken(eecAppToken);

            if (configuration.getCommunicationSpi() == null) {
                TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
                tcpCommunicationSpi.setMessageQueueLimit(1024);
                tcpCommunicationSpi.setSlowClientQueueLimit(1024);
                tcpCommunicationSpi.setSocketWriteTimeout(5000);
                tcpCommunicationSpi.setConnectTimeout(5000);
                tcpCommunicationSpi.setReconnectCount(10);
                configuration.setCommunicationSpi(tcpCommunicationSpi);
            }
            configuration.setClientConnectorConfiguration(null);
            return configuration;
        }
    }

    public CustomerDetails getCustomerDetails(String actorId) {
        if (StringUtils.isEmpty(actorId)) {
            return null;
        }
        try {
            CacheKey cacheKey = new CacheKey(actorId);
            CacheValue cacheValue = customerDetailsCache.get(cacheKey);
            if (cacheValue == null || cacheValue.getValues().length == 0) {
                return null;
            }
            String[] values = cacheValue.getValues();
            if (values.length < 3) {
                log.warn("Incomplete customer details for actorId: {}", actorId);
                return null;
            }
            return new CustomerDetails(values[0], values[1], values[2], values[3], values[4]);
        } catch (Exception e) {
            log.error("Error retrieving customer details for actorId: {}", actorId, e);
            return null;
        }
    }


    public ScenarioMetaParams getScenarioMetaParams(String scenarioName) {
        if (StringUtils.isEmpty(scenarioName)) {
            return null;
        }
        try {
            CacheKey cacheKey = new CacheKey(scenarioName);
            CacheValue cacheValue = scenarioMetaParamsCache.get(cacheKey);
            if (cacheValue == null || cacheValue.getValues().length == 0) {
                return null;
            }
            String[] values = cacheValue.getValues();
            if (values.length < 10) {
                log.warn("Incomplete scenario meta params for scenarioName: {}", scenarioName);
                return null;
            }
            return new ScenarioMetaParams(values[0], values[1], values[2], values[3], values[4],
                    values[5], values[6], values[7], values[8], values[9]);
        } catch (Exception e) {
            log.error("Error retrieving scenario meta params for scenarioName: {}", scenarioName, e);
            return null;
        }
    }


    @PostConstruct
    private void init() throws Exception {
        if (contactPolicyCacheName == null || contactPolicyCacheName.isEmpty()) {
            throw new Exception("Invalid service profile cache name:" + contactPolicyCacheName);
        }
        CacheContainer container = new WaitingCacheContainer(getIgnite(), false);
        contactPolicyCache = container.getCache(contactPolicyCacheName);
        contactPolicyCacheValueDescriptor = container.getCacheValueDescriptor(contactPolicyCacheName);
        trxDailyCache = container.getCache(trxDailyCacheName);
        trxDailyCacheValueDescriptor = container.getCacheValueDescriptor(trxDailyCacheName);
        trxHistCache = container.getCache(trxHistCacheName);
        trxHistCacheValueDescriptor = container.getCacheValueDescriptor(trxHistCacheName);
        customerDetailsCache = container.getCache(customerDetailsCacheName);
        customerDetailsCacheValueDescriptor = container.getCacheValueDescriptor(customerDetailsCacheName);
        scenarioMetaParamsCache = container.getCache(scenarioMetaParamsCacheName);
        scenarioMetaParamsCacheValueDescriptor = container.getCacheValueDescriptor(scenarioMetaParamsCacheName);
    }

    @PreDestroy
    private void destroy() {
        if (ignite != null) {
            ignite.close();
        }
    }
}