package com.evam.marketing.communication.template.configuration;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.io.IOException;

@Configuration
@Slf4j
public class SMSCConfig {
    private SMPPSession smppSession;
    private SMPPConfig smppConfig;

    public SMSCConfig(SMPPConfig smppConfig) {
        this.smppConfig = smppConfig;
    }

    @Bean
    public SMPPSession smppSession() {
        try {
            smppSession = new SMPPSession();
//            smppSession.setMessageReceiverListener(new MessageReceiverListenerImpl());
//            smppSession.setTransactionTimer(smppConfig.getTimeout());
            smppSession.connectAndBind(smppConfig.getHost(), smppConfig.getPort(), new BindParameter(
                    BindType.BIND_TRX,
                    smppConfig.getUsername(),
                    smppConfig.getPassword(),
                    smppConfig.getType(),
                    TypeOfNumber.UNKNOWN,
                    NumberingPlanIndicator.UNKNOWN,
                    null));
        } catch (Exception e){
            log.error("Could not bind to SMSC: {}", e.getMessage());
        }
        return smppSession;
    }

    @PreDestroy
    public void cleanup() {
        if (smppSession != null && smppSession.getSessionState().isBound()) {
            smppSession.unbindAndClose();
        }
    }
}
