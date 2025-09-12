package com.evam.marketing.communication.template.configuration;

import com.evam.evamperformancereport.PerformanceCounter;
import com.evam.evamperformancereport.PerformanceCounterManager;
import com.evam.evamperformancereport.property.PerformanceConfig;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;


@Configuration
@ConfigurationProperties("performance-counter")
@Data
@Order(Ordered.HIGHEST_PRECEDENCE)
@Slf4j
public class PerformanceCounterConfig {

  private long initialDelay = 30;
  private long period = 30;
  private TimeUnit timeUnit = TimeUnit.SECONDS;
  private String scanPackage = "com.evam";
  private boolean countEnabled;
  private boolean schedulerEnabled;

  private PerformanceCounterManager performanceCounterManager;

  @Bean
  public PerformanceCounter performanceCounter() {
    this.performanceCounterManager = new PerformanceCounterManager(
        PerformanceConfig.builder()
            .scanPackage(scanPackage)
            .countEnabled(countEnabled)
            .schedulerEnabled(schedulerEnabled)
            .initialDelay(initialDelay)
            .period(period)
            .timeUnit(timeUnit)
            .build());
    return performanceCounterManager.register();
  }

}
