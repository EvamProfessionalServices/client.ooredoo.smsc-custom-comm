package com.evam.marketing.communication.template.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * Created by cemserit on 11.03.2021.
 */
@Configuration
public class DefaultExecutorConfiguration {

    @Value("${integration.thread-pool.core-pool-size:8}")
    private int corePoolSize;
    @Value("${integration.thread-pool.max-pool-size:16}")
    private int maxPoolSize;
    @Value("${integration.thread-pool.queue-capacity:10000}")
    private int queueCapacity;
    @Value("${integration.thread-pool.prefix:DefaultExecutor}")
    private String threadNamePrefix;

    private static final String THREAD_NAME_SEPARATOR = "-";
    private static final String THREAD_POOL_NAME_VALUE = "pool";
    private static final String THREAD_NAME_VALUE = "thread";

    @Bean("defaultTaskExecutor")
    public ThreadPoolTaskExecutor executor() {
        String integrationSendThreadNamePrefix = getThreadNamePrefix();
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix(integrationSendThreadNamePrefix);
        executor.initialize();
        return executor;
    }

    private String getThreadNamePrefix() {
        return new StringBuilder(threadNamePrefix)
                .append(THREAD_NAME_SEPARATOR)
                .append(THREAD_POOL_NAME_VALUE)
                .append(THREAD_NAME_SEPARATOR)
                .append(corePoolSize)
                .append(THREAD_NAME_SEPARATOR)
                .append(THREAD_NAME_VALUE)
                .append(THREAD_NAME_SEPARATOR)
                .toString();
    }
}
