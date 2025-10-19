package com.evam.marketing.communication.template.configuration;

import jakarta.annotation.PreDestroy;
import org.jsmpp.SMPPConstant;
import org.jsmpp.bean.*;
import org.jsmpp.extra.ProcessRequestException;
import org.jsmpp.session.DataSmResult;
import org.jsmpp.session.MessageReceiverListener;
import org.jsmpp.session.Session;
import org.jsmpp.util.InvalidDeliveryReceiptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class MessageReceiverListenerImpl implements MessageReceiverListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageReceiverListenerImpl.class);

    private final JdbcTemplate jdbcTemplate;
    private final SMPPConfig smppConfig;
//    private static final String UPDATE_DELIVERY_STATUS_SQL =
//            "UPDATE OOREDOO_CUSTOM_SMS_REPORTING SET DELIVERY_STATUS=?, DELIVERY_STATUS_DATE=? WHERE MESSAGE_ID=?";

    private final String updateDeliveryStatusSql;
    private final ConcurrentLinkedQueue<DeliveryReportData> reportQueue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
//    private static final int MAX_RETRIES = 2;
//    private static final int BATCH_SIZE = 200; // Her seferinde en fazla kaç kaydın işleneceği
//    private static final long FLUSH_INTERVAL_MS = 200; // Kuyruğun ne sıklıkla kontrol edileceği (milisaniye)

    private record DeliveryReportData(long messageId, String finalStatus, Timestamp deliveryDate, int retryCount) {}

    public MessageReceiverListenerImpl(JdbcTemplate jdbcTemplate, SMPPConfig smppConfig) {
        this.jdbcTemplate = jdbcTemplate;
        this.smppConfig = smppConfig;
        // 2. CONSTRUCTOR İÇİNDE SQL SORGUSUNU OLUŞTURUN
        // SMPPConfig üzerinden tablo adını alıp sorguyu formatlıyoruz.
        this.updateDeliveryStatusSql = String.format(
                "UPDATE %s SET DELIVERY_STATUS=?, DELIVERY_STATUS_DATE=? WHERE MESSAGE_ID=?",
                smppConfig.getDeliveryReport().getTableName()
        );
        long flushInterval = smppConfig.getBatch().getFlushIntervalMs();
        this.scheduler.scheduleAtFixedRate(this::flushDeliveryReports, 5, flushInterval, TimeUnit.MILLISECONDS);

    }

    @Override
    public void onAcceptDeliverSm(DeliverSm deliverSm) {
        if (MessageType.SMSC_DEL_RECEIPT.containedIn(deliverSm.getEsmClass())) {
            try {
                DeliveryReceipt delReceipt = deliverSm.getShortMessageAsDeliveryReceipt();
                long messageIdAsLong = Long.parseLong(delReceipt.getId()) & 0xffffffff;
                String finalStatus = delReceipt.getFinalStatus().name();
                Timestamp deliveryDate = new Timestamp(delReceipt.getDoneDate().getTime());

                reportQueue.add(new DeliveryReportData(messageIdAsLong, finalStatus, deliveryDate, 0));

            } catch (InvalidDeliveryReceiptException | NumberFormatException e) {
                LOGGER.error("Failed to parse delivery receipt, it will be ignored.", e);
            }
        }
    }

    private void flushDeliveryReports() {
        if (reportQueue.isEmpty()) {
            return;
        }
        final int batchSize = smppConfig.getBatch().getSize();
        // *** DÜZELTME BURADA ***
        // drainTo yerine, kuyruktan BATCH_SIZE kadar elemanı bir döngü ile alıyoruz.
        List<DeliveryReportData> reportsToProcess = new ArrayList<>(batchSize);
        DeliveryReportData report;
            while (reportsToProcess.size() < batchSize && (report = reportQueue.poll()) != null) {
            reportsToProcess.add(report);
        }
        // **********************

        if (reportsToProcess.isEmpty()) {
            return;
        }

        LOGGER.debug("Processing {} delivery reports from the queue...", reportsToProcess.size());
        LOGGER.debug("UPDATE_DELIVERY_STATUS_SQL: {}", this.updateDeliveryStatusSql);
        int[] updateResults = jdbcTemplate.batchUpdate(this.updateDeliveryStatusSql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                DeliveryReportData report = reportsToProcess.get(i);
                ps.setString(1, report.finalStatus());
                ps.setTimestamp(2, report.deliveryDate());
                ps.setLong(3, report.messageId());

                // ================= YENİ EKLENEN LOG =================
                // Sorguyu ve parametreleri manuel olarak formatlayıp DEBUG seviyesinde logluyoruz.
                String formattedSql = String.format(
//                        "Executing Batch SQL [%d/%d]: UPDATE OOREDOO_CUSTOM_SMS_REPORTING SET DELIVERY_STATUS='%s', DELIVERY_STATUS_DATE='%s' WHERE MESSAGE_ID=%d",
                        "Executing Batch SQL [%d/%d]: UPDATE %s SET DELIVERY_STATUS='%s', DELIVERY_STATUS_DATE='%s' WHERE MESSAGE_ID=%d",
                        i + 1,
                        reportsToProcess.size(),
                        smppConfig.getDeliveryReport().getTableName(),
                        report.finalStatus(),
                        report.deliveryDate(),
                        report.messageId()
                );
                LOGGER.debug(formattedSql);
            }

            @Override
            public int getBatchSize() {
                return reportsToProcess.size();
            }
        });

        for (int i = 0; i < updateResults.length; i++) {
            if (updateResults[i] == 0) {
                DeliveryReportData failedReport = reportsToProcess.get(i);
                final int maxRetries = smppConfig.getBatch().getMaxRetries();
                if (failedReport.retryCount() < maxRetries) {
                    reportQueue.add(new DeliveryReportData(
                            failedReport.messageId(),
                            failedReport.finalStatus(),
                            failedReport.deliveryDate(),
                            failedReport.retryCount() + 1
                    ));
                    LOGGER.debug("Message ID {} not found, re-queueing for retry (attempt {}).", failedReport.messageId(), failedReport.retryCount() + 1);
                } else {
                    LOGGER.error("FAIL: Could not find message with numeric ID '{}' after {} retries. Giving up.", failedReport.messageId(), maxRetries);
                }
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        // Uygulama kapanmadan önce kuyrukta kalan son elemanları da işlemeyi dene
        flushDeliveryReports();
        scheduler.shutdown();
    }

    // Diğer metodlar aynı kalacak
    @Override
    public void onAcceptAlertNotification(AlertNotification alertNotification) { LOGGER.warn("AlertNotification received: {}", alertNotification); }
    @Override
    public DataSmResult onAcceptDataSm(DataSm dataSm, Session source) throws ProcessRequestException {
        LOGGER.warn("DataSm received but not implemented.");
        throw new ProcessRequestException("data_sm not implemented", SMPPConstant.STAT_ESME_RINVCMDID);
    }
}
//package com.evam.marketing.communication.template.configuration;
//
//import org.jsmpp.SMPPConstant;
//import org.jsmpp.bean.*;
//import org.jsmpp.extra.ProcessRequestException;
//import org.jsmpp.session.DataSmResult;
//import org.jsmpp.session.MessageReceiverListener;
//import org.jsmpp.session.Session;
//import org.jsmpp.util.InvalidDeliveryReceiptException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.jdbc.core.JdbcTemplate;
//import org.springframework.stereotype.Service;
//import java.sql.Timestamp;
//
//@Service
//public class MessageReceiverListenerImpl implements MessageReceiverListener {
//    private static final Logger LOGGER = LoggerFactory.getLogger(MessageReceiverListenerImpl.class);
//    private final JdbcTemplate jdbcTemplate;
//    private static final String UPDATE_DELIVERY_STATUS_SQL =
//            "UPDATE OOREDOO_CUSTOM_SMS_REPORTING SET DELIVERY_STATUS=?, DELIVERY_STATUS_DATE=? WHERE MESSAGE_ID=?";
//
//    public MessageReceiverListenerImpl(JdbcTemplate jdbcTemplate) {
//        this.jdbcTemplate = jdbcTemplate;
//    }
//
//    @Override
//    public void onAcceptDeliverSm(DeliverSm deliverSm) {
//        LOGGER.debug(">>> onAcceptDeliverSm triggered. A message was received from SMPP server.");
//        if (MessageType.SMSC_DEL_RECEIPT.containedIn(deliverSm.getEsmClass())) {
//            try {
//                LOGGER.debug("========== DELIVERY REPORT RECEIVED ==========");
//                DeliveryReceipt delReceipt = deliverSm.getShortMessageAsDeliveryReceipt();
//
//                long messageIdAsLong = Long.parseLong(delReceipt.getId()) & 0xffffffff;
//                String finalStatus = delReceipt.getFinalStatus().name();
//                Timestamp deliveryDate = new Timestamp(delReceipt.getDoneDate().getTime());
//
//                LOGGER.debug("Attempting to update DB for numeric messageId: '{}' with status: '{}'", messageIdAsLong, finalStatus);
//
//                int affectedRows = jdbcTemplate.update(
//                        UPDATE_DELIVERY_STATUS_SQL,
//                        finalStatus,
//                        deliveryDate,
//                        messageIdAsLong
//                );
//
//                if (affectedRows > 0) {
//                    LOGGER.debug("SUCCESS: DB updated for message '{}'. Status: {}", messageIdAsLong, finalStatus);
//                } else {
//                    //şu değişcek
//                    LOGGER.warn("FAIL: Could not find message with numeric ID '{}' to update status.", messageIdAsLong);
//                }
//            } catch (InvalidDeliveryReceiptException | NumberFormatException e) {
//                LOGGER.error("Failed to parse delivery receipt or its ID.", e);
//            }
//        } else {
//            LOGGER.debug("Regular SMS received: {}", new String(deliverSm.getShortMessage()));
//        }
//    }
//
//    // Diğer metodlar aynı kalacak
//    @Override
//    public void onAcceptAlertNotification(AlertNotification alertNotification) { LOGGER.warn("AlertNotification received: {}", alertNotification); }
//    @Override
//    public DataSmResult onAcceptDataSm(DataSm dataSm, Session source) throws ProcessRequestException {
//        LOGGER.warn("DataSm received but not implemented.");
//        throw new ProcessRequestException("data_sm not implemented", SMPPConstant.STAT_ESME_RINVCMDID);
//    }
//}