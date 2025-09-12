package com.evam.marketing.communication.template.cacheserver;

import com.evam.cache.client.CacheContainer;
import com.evam.cache.client.CacheValueDescriptor;
import com.evam.cache.client.WaitingCacheContainer;
import com.evam.cache.common.CustomTcpDiscoverySpi;
import com.evam.cache.common.IgniteConfigurationReader;
import com.evam.cache.model.CacheKey;
import com.evam.cache.model.CacheValue;
import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.client.model.Parameter;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
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

@Service
@Slf4j
public class CacheQueryService {
    private Ignite ignite;
    private IgniteCache<CacheKey, CacheValue> contactPolicyCache;
    private CacheValueDescriptor contactPolicyCacheValueDescriptor;
    private IgniteCache<CacheKey, CacheValue> trxDailyCache;
    private CacheValueDescriptor trxDailyCacheValueDescriptor;
    private IgniteCache<CacheKey, CacheValue> trxHistCache;
    private CacheValueDescriptor trxHistCacheValueDescriptor;

    @Value("${eec.pool-cache-name}")
    private String contactPolicyCacheName;
    @Value("${eec.trx-daily-cache}")
    private String trxDailyCacheName;
    @Value("${eec.trx-hist-cache}")
    private String trxHistCacheName;

    @Value("${eec.ignite-client-file-path}")   private String igniteClientFilePath;
    @Value("${eec.app-token}")  private String eecAppToken;

    public ContactPolicyLimits getCacheValues(CustomCommunicationRequest req, Map<String, String> requiredFields){
        CacheKey cacheKey = new CacheKey(req.getActorId(),"SMS",requiredFields.get("messageType"));
        List<String> limits = Collections.emptyList();
        List<String> dailyTransactions = Collections.emptyList();
        List<String> histTransactions = Collections.emptyList();
        try {
            limits = Arrays.asList(contactPolicyCache.get(cacheKey).getValues());
        }catch (Exception e){
            log.error("Cache read error for limits key: {}", cacheKey, e);
            return null;
        }
        try {
            dailyTransactions = Arrays.asList(trxDailyCache.get(cacheKey).getValues());
        }catch (Exception e){
            log.warn("Cache read error for dailyTransactions key: {}", cacheKey, e);
        }
        try {
            histTransactions = Arrays.asList(trxHistCache.get(cacheKey).getValues());
        }catch (Exception e){
            log.warn("Cache read error for histTransactions key: {}", cacheKey, e);
        }
        try {
            ContactPolicyLimits contactPolicyLimits = new ContactPolicyLimits(Integer.valueOf(limits.get(1)), Integer.valueOf(limits.get(2)),
                    Integer.valueOf(limits.get(3)), dailyTransactions.isEmpty() ? 0 : Integer.valueOf(dailyTransactions.get(0)),
                    histTransactions.isEmpty() ? 0 : Integer.valueOf(histTransactions.get(0)), histTransactions.isEmpty() ? 0 : Integer.valueOf(histTransactions.get(1)));
            return contactPolicyLimits;
        }catch (Exception e){
            log.error("Conversion error for contactPolicyLimits key: {}", cacheKey, e);
            return null;
        }
    }

    public void putCacheValue(CustomCommunicationRequest req, Map<String, String> requiredFields, String updatedDailySum){
        try {
            CacheKey cacheKey = new CacheKey(req.getActorId(), "SMS", requiredFields.get("messageType"));
            CacheValue cacheValue = new CacheValue(new String[]{updatedDailySum}, 1, 1);
            trxDailyCache.put(cacheKey, cacheValue);
        }catch (Exception e){
            log.error("Cache write error for dailyTransactions key: {}", req.getActorId(), e);
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
    }

    @PreDestroy
    private void destroy() {
        if (ignite != null) {
            ignite.close();
        }
    }
}
