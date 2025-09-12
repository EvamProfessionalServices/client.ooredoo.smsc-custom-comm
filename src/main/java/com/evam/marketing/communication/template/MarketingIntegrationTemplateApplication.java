package com.evam.marketing.communication.template;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@Slf4j
//@ComponentScan(basePackages = {"com.evam.marketing.communication.template",
//    "com.evam.cloud.marketing.survey.link.client"})
public class MarketingIntegrationTemplateApplication {

  public static void main(String[] args) {
    SpringApplication.run(MarketingIntegrationTemplateApplication.class, args);
  }
}
