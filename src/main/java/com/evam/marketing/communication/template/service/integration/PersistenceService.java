package com.evam.marketing.communication.template.service.integration;

import com.evam.evamperformancereport.PerformanceCounter;
import com.evam.evamperformancereport.model.PerformanceModelType;
import com.evam.marketing.communication.template.configuration.PersistentConfig;
import com.evam.marketing.communication.template.configuration.SMPPConfig;
import com.evam.marketing.communication.template.configuration.SMSCConfig;
import com.evam.marketing.communication.template.service.client.AbstractCommunicationClient;
import com.evam.marketing.communication.template.service.client.MockCommunicationClient;
import com.evam.marketing.communication.template.service.event.KafkaProducerService;
import com.evam.marketing.communication.template.service.integration.model.request.CommunicationRequest;
import com.evam.marketing.communication.template.service.integration.model.response.CommunicationResponse;
import lombok.extern.slf4j.Slf4j;
import net.fellbaum.jemoji.EmojiManager;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.*;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.util.AbsoluteTimeFormatter;
import org.jsmpp.util.TimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@Slf4j
public class PersistenceService extends AbstractCommunicationClient{
    private final LinkedBlockingQueue<ServiceRequest> queue;
    private final ScheduledExecutorService scheduledExecutorService;
    private final PersistentConfig config;
    private static final String CLASS_SIMPLE_NAME = PersistenceService.class.getSimpleName();
    private final PerformanceCounter performanceCounter;
    private final SMPPSession smppSession;
    private static final TimeFormatter TIME_FORMATTER = new AbsoluteTimeFormatter();
    private static final String ENG = "1";
    private static final String AR = "2";
    private static final String ENG_AR = "3";
    private static final String SUCCESS = "SUCCESS";
    private static final String CONTROL_GROUP = "CONTROL_GROUP";
    private static final String QUOTA_EXCEEDED = "QUOTA_EXCEEDED";
    private static final Random RANDOM = new Random();
    private final Reporting reporting;
    private final SMPPConfig smppConfig;

    public PersistenceService(KafkaProducerService kafkaProducerService, PersistentConfig config, PerformanceCounter performanceCounter,
                              SMPPSession smppSession, Reporting reporting, SMPPConfig smppConfig) {
        super(kafkaProducerService);
        this.config = config;
        queue = new LinkedBlockingQueue<>(config.getPersistJobBufferSize());
        this.reporting = reporting;
        this.smppConfig = smppConfig;
        ThreadFactory threadFactory = new CustomizableThreadFactory(
                "Persister-");
        scheduledExecutorService = Executors.newScheduledThreadPool(config.getPersistPoolSize(),
                threadFactory);
        for (int i = 0; i < config.getPersistPoolSize(); i++) {
            scheduledExecutorService.scheduleAtFixedRate(new BatchRunnable(), 0, 1,
                    TimeUnit.NANOSECONDS);
        }
        this.performanceCounter = performanceCounter;
        this.smppSession = smppSession;
    }

    public void enqueue (ServiceRequest request){
        try {
            queue.put(request);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void persist(List<ServiceRequest> requests) {
        if (requests.isEmpty()) {
            return;
        }
        try{
            //QUOTA CHECK
            List<ServiceRequest> quotaLimitExceededRequests = requests.stream()
                    .filter(r -> {
                        String quoutaCheck = r.getComputed().getQuotaCheck();
                        if ("OK".equals(quoutaCheck))
                        {
                            log.debug("Quota actor will be inserted into Evam Repo. ActorId: {} ", r.getBase().getActorId());
                            return false;
                        }
                        return true;
                    }).toList();
            if(!quotaLimitExceededRequests.isEmpty()) {
                log.info("Persisting quota actors of " + quotaLimitExceededRequests.size() + " requests to Evam Repo.");
                reporting.saveToDatabase(quotaLimitExceededRequests, QUOTA_EXCEEDED);
                quotaLimitExceededRequests.forEach( x-> sendEvent(generateFailCommunicationResponse(x.getOriginalRequest(),"EVAM",x.getComputed().getQuotaCheck()).toEvent()));
            }
            requests.removeAll(quotaLimitExceededRequests);
            //REMOVE TEST ACTORS
            List<ServiceRequest> testRequests = requests.stream()
                    .filter(r -> {
                        boolean testModeEnabled = r.getComputed().isTestModeEnabled();
                        if (testModeEnabled)
                        {
                            log.debug("Test actor will be inserted into Evam Repo. ActorId: {} ", r.getBase().getActorId());
                        }
                        return testModeEnabled;
                    }).toList();
            if(!testRequests.isEmpty()) {
                log.info("Persisting test actors of " + testRequests.size() + " requests to Evam Repo.");
                reporting.saveToDatabase(testRequests, SUCCESS);
                testRequests.forEach( x-> sendEvent(generateSuccessCommunicationResponse(x.getOriginalRequest(),"EVAM","Successfully inserted into Oracle" ).toEvent()));
            }
            requests.removeAll(testRequests);
            //REMOVE CONTROL GROUP ACTORS
            //CONTROL GROUP ACTORS ARE NOT TEST ACTORS, THEY ARE REAL ACTORS WHO ARE
            //PART OF A CONTROL GROUP
            List<ServiceRequest> controlGroupRequests = requests.stream()
                    .filter(r -> {
                        boolean controlGroup = r.getComputed().isControlGroup();
                        if (controlGroup)
                        {
                            log.debug("Control group actor will be inserted into Evam Repo. ActorId: {} ", r.getBase().getActorId());
                        }
                        return controlGroup;
                    }).toList();
            if(!controlGroupRequests.isEmpty()) {
                log.info("Persisting control group actors of " + controlGroupRequests.size() + " requests to Evam Repo.");
                reporting.saveToDatabase(controlGroupRequests, CONTROL_GROUP);
                controlGroupRequests.forEach( x-> sendEvent(generateSuccessCommunicationResponse(x.getOriginalRequest(),"EVAM","Successfully inserted into Oracle" ).toEvent()));
            }
            requests.removeAll(controlGroupRequests);

            if(!requests.isEmpty()) {
                requests.forEach(x -> {

                    byte[] messageBytes = {};
                    Alphabet dataCoding = Alphabet.ALPHA_DEFAULT;
                    RawDataCoding rawDataCoding = null;
                    int segmentSize = 153;

                    String message = "";
                    List<byte[]> bytes = new ArrayList<>();
                    try {
                        boolean isEmoji = false;
                        if(ENG.equals(x.getComputed().getLanguagePreference())){
                            if(EmojiManager.containsEmoji(x.getComputed().getTextEng())){
                                isEmoji = true;
                                boolean isGsm7 = isGsm7Compatible(x.getComputed().getTextEng());
                                messageBytes = x.getComputed().getTextEng().getBytes(StandardCharsets.UTF_16BE); // GSM-7 fallback approximation
                                dataCoding = Alphabet.ALPHA_UCS2; // GSM default
                                segmentSize = 70; // 160 - UDH
                                message = x.getComputed().getTextEng();
                                rawDataCoding = new RawDataCoding ((byte) 8);
                                x.getComputed().setMessage(x.getComputed().getTextEng());
                            }else{
                                boolean isGsm7 = isGsm7Compatible(x.getComputed().getTextEng());
                                messageBytes = x.getComputed().getTextEng().getBytes(StandardCharsets.ISO_8859_1); // GSM-7 fallback approximation
                                message = x.getComputed().getTextEng();
                                rawDataCoding = new RawDataCoding ((byte) 0);
                                x.getComputed().setMessage(x.getComputed().getTextEng());
                            }
                        }
                        else if(AR.equals(x.getComputed().getLanguagePreference())){
                            boolean isGsm7 = isGsm7Compatible(x.getComputed().getTextAr());
                            messageBytes = x.getComputed().getTextAr().getBytes(StandardCharsets.UTF_16BE);
                            dataCoding = Alphabet.ALPHA_UCS2; // GSM default
                            segmentSize = 70; // 160 - UDH
                            message = x.getComputed().getTextAr();
                            rawDataCoding = new RawDataCoding ((byte) 8);
                            x.getComputed().setMessage(x.getComputed().getTextAr());
                        }
                        else if(ENG_AR.equals(x.getComputed().getLanguagePreference())){
                            boolean isGsm7 = isGsm7Compatible(x.getComputed().getTextMixed());
                            messageBytes = x.getComputed().getTextMixed().getBytes(StandardCharsets.UTF_16BE);
                            dataCoding = Alphabet.ALPHA_UCS2; // GSM default
                            segmentSize = 70; // 160 - UDH
                            message = x.getComputed().getTextMixed();
                            rawDataCoding = new RawDataCoding ((byte) 8);
                            x.getComputed().setMessage(x.getComputed().getTextMixed());
                        }
                        boolean flash = x.getComputed().isFlashSmsEnabled();
                        MessageClass messageClass = flash ? MessageClass.CLASS0 : MessageClass.CLASS1;
                        GeneralDataCoding generalDataCoding = new GeneralDataCoding(dataCoding,messageClass,false);
                        //SENDER DYNAMIC
                        String messageId = "";

                        // Check if the SMPP session is bound
                        if (!smppSession.getSessionState().isBound()) {
                            log.error("SMPP session is not bound, trying to reconnect...");

                            try {
                                // Rebind the SMPP session using the connectAndBind method
                                smppSession.connectAndBind(
                                        smppConfig.getHost(), // SMPP Host (use the correct host)
                                        smppConfig.getPort(), // SMPP Port (use the correct port)
                                        new BindParameter(
                                                BindType.BIND_TRX, // Using transceiver mode (TRX)
                                                smppConfig.getUsername(), // Your SMPP username
                                                smppConfig.getPassword(), // Your SMPP password
                                                smppConfig.getType(), // Optional: System Type (if applicable)
                                                TypeOfNumber.UNKNOWN, // Sender number type (you can adjust based on your requirements)
                                                NumberingPlanIndicator.UNKNOWN, // Sender numbering plan indicator (adjust as needed)
                                                null // Optional: Address range (null if not used)
                                        )
                                );

                                log.debug("SMPP session successfully reconnected and bound.");

                            } catch (Exception e) {
                                log.error("Failed to rebind SMPP session", e);
                                // Handle failure: decide if you want to retry or abort the operation
                                return; // Exit or handle as needed
                            }
                        }
                        RawDataCoding flashDataCoding = null;
                        if(flash){
                            log.debug("Flash sms will be sent");
                            flashDataCoding = new RawDataCoding ((byte) 0x10);
                        }
                        if (messageBytes.length <= segmentSize) {

                             messageId = smppSession.submitShortMessage(
                                    "CMT",
                                    TypeOfNumber.ALPHANUMERIC, NumberingPlanIndicator.UNKNOWN, x.getComputed().getSenderId(),
                                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, x.getComputed().getTo(),
                                    new ESMClass(), (byte) 0, (byte) 1,
                                    TIME_FORMATTER.format(new Date()), null,
                                    new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT),
                                    (byte) 0,
                                    flash ? generalDataCoding : rawDataCoding,
                                    (byte) 0,
                                    messageBytes).getMessageId();
                        } else {

                            List<byte[]> messageBytesList = new ArrayList<>();



                            messageBytesList.add(messageBytes);

                            // Multipart message
                            byte referenceNumber = (byte) RANDOM.nextInt(255);
                            int totalSegments = (int) Math.ceil((double) messageBytes.length / segmentSize);

                            OptionalParameter sarMsgRefNum = OptionalParameters.newSarMsgRefNum((short)RANDOM.nextInt());
                            OptionalParameter sarTotalSegments = OptionalParameters.newSarTotalSegments(totalSegments);


                            for (int i = 0; i < totalSegments; i++) {
                                final int seqNum = i + 1;

                                int start = i * segmentSize;
                                int length = Math.min(segmentSize, messageBytes.length - start);
                                byte[] segment = Arrays.copyOfRange(messageBytes, start, start + length);

                                OptionalParameter sarSegmentSeqnum = OptionalParameters.newSarSegmentSeqnum(seqNum);

                                log.info("segment " + seqNum + " - " + Arrays.toString(segment));
                                messageId = smppSession.submitShortMessage(
                                        "CMT",
                                        TypeOfNumber.ALPHANUMERIC, NumberingPlanIndicator.UNKNOWN, x.getComputed().getSenderId(),
                                        TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.UNKNOWN, x.getComputed().getTo(),
                                        new ESMClass(), // UDHI flag set
                                        (byte) 0, (byte) 1,
                                        TIME_FORMATTER.format(new Date()), null,
                                        new RegisteredDelivery(SMSCDeliveryReceipt.DEFAULT),
                                        (byte) 0,
                                         flash ? generalDataCoding : "1".equals(x.getComputed().getLanguagePreference()) && !isEmoji ? DataCodings.ZERO : new RawDataCoding((byte) 8),
                                        (byte) 0,segment,sarMsgRefNum, sarSegmentSeqnum, sarTotalSegments
                                ).getMessageId();

                            }
                        }
                        x.setMessageId(messageId);
                        log.info("Message submitted, message_id is {}", messageId);
                        sendEvent(generateSuccessCommunicationResponse(x.getOriginalRequest(), "EVAM", "Successfully inserted into Oracle").toEvent());
                        performanceCounter.increment(CLASS_SIMPLE_NAME, PerformanceModelType.SUCCESS);
                    } catch (PDUException | ResponseTimeoutException | InvalidResponseException | NegativeResponseException |
                            IOException e) {
                        log.error("Error while sending message to MSISDN : {} Reason : {}",x.getBase().getActorId(), e.getMessage());
                    }
                });
                reporting.saveToDatabase(requests, "SUCCESS");
                log.debug("Persisted {} requests into the Oracle MV.", requests.size());
            }

        } catch (Exception e) {
            try {
                reporting.saveToDatabase(requests, "FAIL");
                log.error("Error while persisting requests to Oracle. Requests: {}", requests, e);
                requests.forEach(x -> sendEvent(generateFailCommunicationResponse(x.getOriginalRequest(), "EVAM", e.getMessage()).toEvent()));
                performanceCounter.increment(CLASS_SIMPLE_NAME, PerformanceModelType.ERROR);
            }catch (Exception ex){
                log.error("Error while persisting requests to Oracle. Requests: {}", requests, e);
                requests.forEach(x -> sendEvent(generateFailCommunicationResponse(x.getOriginalRequest(), "EVAM", e.getMessage()).toEvent()));
                performanceCounter.increment(CLASS_SIMPLE_NAME, PerformanceModelType.ERROR);
                return;
            }
        }
    }
    private boolean isGsm7Compatible(String message) {
        // Simplified checker: if all characters are basic Latin
        for (char c : message.toCharArray()) {
            if (c > 127) return false; // conservative GSM-7 check
        }
        return true;
    }

    private class BatchRunnable implements Runnable {
        AtomicLong previousRunTime = new AtomicLong(System.currentTimeMillis());
        @Override
        public void run() {

            ArrayList<ServiceRequest> drainList = new ArrayList<>();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    ServiceRequest poll = queue.poll(1000, TimeUnit.MILLISECONDS);
                    if (poll != null) {
                        drainList.add(poll);
                    }

                    boolean timedOut = System.currentTimeMillis() - previousRunTime.get()
                            > TimeUnit.SECONDS.toMillis(10);
                    if (drainList.size()
                            >= config.getPersistJobBufferSize() / config.getPersistPoolSize()
                            || timedOut) {
                        persist(drainList);
                        previousRunTime.set(System.currentTimeMillis());
                        break;
                    }
                } catch (InterruptedException e) {
                    persist(drainList);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("Error while persisting", e);
                }
            }
        }
    }

    @Override
    public CommunicationResponse send(CommunicationRequest communicationRequest){
        return null;
    };

    @Override
    public String getProvider() {
        return null;
    }
}
