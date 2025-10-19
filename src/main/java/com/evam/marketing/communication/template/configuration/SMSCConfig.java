package com.evam.marketing.communication.template.configuration;

import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class SMSCConfig {

    private final SMPPConfig smppConfig;
    private final MessageReceiverListenerImpl messageReceiverListener;

    public SMSCConfig(SMPPConfig smppConfig, MessageReceiverListenerImpl messageReceiverListener) {
        this.smppConfig = smppConfig;
        this.messageReceiverListener = messageReceiverListener;
    }

    @Bean(destroyMethod = "unbindAndClose")
    public SMPPSession smppSession() {
        log.debug("Creating and configuring the single SMPPSession bean...");
        SMPPSession session = new SMPPSession();
        try {
            session.setMessageReceiverListener(this.messageReceiverListener);
            session.setTransactionTimer(smppConfig.getTimeout());
            log.debug("MessageReceiverListener attached to the session.");
            session.setPduProcessorDegree(5);

            log.debug("Connecting and binding to SMPP server at {}:{}...", smppConfig.getHost(), smppConfig.getPort());
            session.connectAndBind(smppConfig.getHost(), smppConfig.getPort(), new BindParameter(
                    BindType.BIND_TRX,
                    smppConfig.getUsername(),
                    smppConfig.getPassword(),
                    smppConfig.getType(),
                    TypeOfNumber.UNKNOWN,
                    NumberingPlanIndicator.UNKNOWN,
                    null));

            log.debug("SMPP Session successfully bound! Session State: {}", session.getSessionState());

        } catch (Exception e) {
            log.error("CRITICAL ERROR: Could not bind to SMSC during startup!", e);
        }
        return session;
    }
}

