package com.evam.marketing.communication.template.configuration;

import com.evam.encryptor.utility.CryptUtils;
import lombok.extern.slf4j.Slf4j;
import org.jasypt.encryption.StringEncryptor;
import org.springframework.context.annotation.Configuration;

/*
 * Created by Mehmet Gökhan MERAL at 06.05.2021
 */
@Configuration
@Slf4j
public class EvamEncryptor implements StringEncryptor {
    public String encrypt(String data) {
        return CryptUtils.encrypt(data);
    }

    public String decrypt(String encryptedData) {
        return CryptUtils.decrypt(encryptedData);
    }
}