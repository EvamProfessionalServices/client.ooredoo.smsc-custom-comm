package com.evam.marketing.communication.template.configuration;

import com.ulisesbocchio.jasyptspringboot.EncryptablePropertyResolver;
import com.ulisesbocchio.jasyptspringboot.resolver.DefaultPropertyResolver;
import org.jasypt.encryption.StringEncryptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.Environment;

@Configuration
@Order(Ordered.HIGHEST_PRECEDENCE)
public class EncryptConfigurationConfig {
    @Bean
    public StringEncryptor stringEncryptor(@Autowired EvamEncryptor evamEncryptor) {
        return evamEncryptor;
    }

    @Bean
    public EncryptablePropertyResolver encryptablePropertyResolver(@Autowired EvamEncryptor jasyptStringEncryptor,
                                                                   @Autowired Environment environment) {
        return new DefaultPropertyResolver(jasyptStringEncryptor, environment);
    }
}
