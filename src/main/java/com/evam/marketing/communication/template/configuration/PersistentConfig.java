package com.evam.marketing.communication.template.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "persister")
@Data
public class PersistentConfig {
    private String timeStampFormatOracle;
    private String timeStampFormatPostgres;
    private int persistJobBufferSize;
    private int persistPoolSize;
    private String testActors;
    private String oracleSchemaName;
    private String postgresqlSchemaName;
    private String insertSql;
}
