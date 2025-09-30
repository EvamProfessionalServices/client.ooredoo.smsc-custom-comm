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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    private IgniteCache<CacheKey, CacheValue> actorSegmentCache;
    private CacheValueDescriptor actorSegmentCacheValueDescriptor;

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
    @Value("${eec.actor-segment-cache-name}")
    private String actorSegmentCacheName;

    @Value("${eec.ignite-client-file-path}")
    private String igniteClientFilePath;
    @Value("${eec.app-token}")
    private String eecAppToken;

    /**
     * Belirtilen filtreye göre transaction verilerindeki ilgili alanları toplar.
     * EecCacheGetCommunicationClient tarafından kullanılabilmesi için 'public' yapıldı.
     */
    public int sumTransactionValues(List<Map<String, Object>> transactions, Predicate<Map<String, Object>> filter, String key) {
        if (transactions == null || transactions.isEmpty()) {
            return 0;
        }
        return transactions.stream()
                .filter(filter)
                .mapToInt(tx -> tx.containsKey(key) ? ((Number) tx.get(key)).intValue() : 0)
                .sum();
    }
    /**
     * Belirtilen filtreye uyan ilk transaction kaydını bulur ve istenen alandaki (key) değeri döner.
     * Toplama yapmak yerine doğrudan spesifik bir kaydın değerini okumak için kullanılır.
     * @param transactions İşlem listesi (daily veya hist)
     * @param filter İstenen kaydı bulmak için kullanılacak filtre (predicate)
     * @param key Değeri okunacak alanın adı ("SENT_COUNT", "WEEKLY_SUM", vb.)
     * @return Bulunan kayıttaki ilgili alanın integer değeri. Kayıt veya alan bulunamazsa 0 döner.
     */
    public int getSpecificTransactionValue(List<Map<String, Object>> transactions, Predicate<Map<String, Object>> filter, String key) {
        if (transactions == null || transactions.isEmpty()) {
            return 0;
        }

        // Stream'i kullanarak filtreye uyan ilk elemanı bir Optional olarak al
        Optional<Map<String, Object>> foundOptional = transactions.stream()
                .filter(filter)
                .findFirst();

        // Eğer filtreye uyan bir kayıt bulunamadıysa, 0 dön
        if (foundOptional.isEmpty()) {
            return 0;
        }

        // Eğer kayıt bulunduysa, Optional'dan Map'i çıkar
        Map<String, Object> transactionMap = foundOptional.get();

        // Map içerisinden istenen 'key' ile değeri al (Object tipinde gelecektir)
        Object value = transactionMap.get(key);

        // Değerin null olmadığını ve bir sayı (Number) olduğunu kontrol et
        if (value instanceof Number) {
            // Güvenli bir şekilde integer'a çevir ve döndür
            return ((Number) value).intValue();
        }

        // Eğer değer null ise veya sayı değilse, 0 dön
        return 0;
    }

    /**
     * EecCacheGetCommunicationClient'ın hiyerarşik kural araması için gereken yardımcı metod.
     * Belirtilen anahtara ait policy değerlerini döner.
     */
    public List<String> getPolicyValues(CacheKey key) {
        try {
            CacheValue value = contactPolicyCache.get(key);
            if (value != null && value.getValues() != null && value.getValues().length > 0) {
                return Arrays.asList(value.getValues());
            }
        } catch (Exception e) {
            log.error("Error getting policy values for key: {}", key, e);
        }
        return null; // Bulunamazsa veya hata olursa null dön.
    }

    //GET SEGMENT NAME BY ACTOR ID
    public String getSegmentNameByActorId(String actorId) {
        try {
            CacheKey cacheKey = new CacheKey(actorId);
            CacheValue cacheValue = actorSegmentCache.get(cacheKey);
            List<String> values = Arrays.asList(cacheValue.getValues());
            String segmentName = values.get(0);
            return segmentName;
        }catch (Exception e){
            log.error("Error getting segment name for actorId: {}", actorId, e);
            return null;
        }
    }

    /**
     * Kota kontrolü başarılı olduğunda transaction cache'ini güncelleyen metod.
     */
    public void putCacheValue(CustomCommunicationRequest req, Map<String, String> requiredFields, String updatedDailySum) {
        try {
            CacheKey cacheKey = new CacheKey(req.getActorId());
            ObjectMapper objectMapper = new ObjectMapper();

            CacheValue existingValue = trxDailyCache.get(cacheKey);
            List<Map<String, Object>> transactionsList;

            if (existingValue == null || existingValue.getValues().length == 0 || existingValue.getValues()[0].isEmpty()) {
                transactionsList = new ArrayList<>();
            } else {
                String jsonString = existingValue.getValues()[0];
                transactionsList = objectMapper.readValue(jsonString, new TypeReference<List<Map<String, Object>>>() {
                });
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
            if (values.length < 5) { // Assuming 5 fields are expected based on constructor
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

    private Ignite getIgnite() throws IOException {
        do {
            IgniteConfiguration igniteConfiguration = getIgniteConfiguration();
            try {
                ignite = Ignition.getOrStart(igniteConfiguration);
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
        actorSegmentCache = container.getCache(actorSegmentCacheName);
        actorSegmentCacheValueDescriptor = container.getCacheValueDescriptor(actorSegmentCacheName);
    }

    @PreDestroy
    private void destroy() {
        if (ignite != null) {
            ignite.close();
        }
    }
}