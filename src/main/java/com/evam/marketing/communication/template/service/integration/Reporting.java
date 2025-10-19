package com.evam.marketing.communication.template.service.integration;

import com.evam.marketing.communication.template.cacheserver.CacheQueryService;
import com.evam.marketing.communication.template.cacheserver.DTO.CustomerDetails;
import com.evam.marketing.communication.template.cacheserver.DTO.ScenarioMetaParams;
import com.evam.marketing.communication.template.configuration.PersistentConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
@Service
public class Reporting {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private PersistentConfig persistentConfig;
    private static final String ENG = "1";
    private static final String AR = "2";
    private static final String ENG_AR = "3";
    private static final String CONTROL_GROUP = "CONTROL_GROUP";
    private static final String QUOTA_EXCEEDED = "QUOTA_EXCEEDED";
    private static final String CONTACT_POLICY = "CONTACT_POLICY";
    private static final String SUCCESS = "SUCCESS";
    private static final String REJECTED = "REJECTED";
    private static final String EMPTY_STRING = "";
    private final CacheQueryService cacheQueryService;

    public Reporting(PersistentConfig persistentConfig, CacheQueryService cacheQueryService) {
        this.persistentConfig = persistentConfig;
        this.cacheQueryService = cacheQueryService;
    }

    public void saveToDatabase(List<ServiceRequest> requests, String status) {
        String query = persistentConfig.getInsertSql();
        jdbcTemplate.batchUpdate(query, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ServiceRequest request = requests.get(i);
                setPs(ps,request, status);
            }

            @Override
            public int getBatchSize() {
                return requests.size();
            }
        });
    }

    public void setPs(PreparedStatement ps, ServiceRequest request, String status) {
        try {
            CustomerDetails customerDetails = cacheQueryService.getCustomerDetails(request.getBase().getActorId());
            String contractId = customerDetails != null ? customerDetails.getContractid() : EMPTY_STRING;
            String customerType = customerDetails != null ? customerDetails.getCustomertype() : EMPTY_STRING;
            String tgCgFlag = customerDetails != null ? "1".equals(customerDetails.getUcgflag()) ? "0" : "1" : EMPTY_STRING;
            String businessSubGroup = customerDetails != null ? customerDetails.getCustomertype() : EMPTY_STRING;
            ScenarioMetaParams scenarioMetaParams = cacheQueryService.getScenarioMetaParams(request.getBase().getScenarioName());
            String altContactNumber = scenarioMetaParams != null ? "PREPAIDCVM".equals(scenarioMetaParams.getBUSINESS_GROUP())
                    ? customerDetails.getMsisdnalt0() : customerDetails.getMsisdn_cont_base_ls() : EMPTY_STRING;
            String camId = scenarioMetaParams != null ? scenarioMetaParams.getMAIN_CAMPAIGN_NAME() + "_" + scenarioMetaParams.getVERSION() : EMPTY_STRING;
            LocalDate now = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
            String currentDate = now.format(formatter);
            String usecaseTreatmentCode = scenarioMetaParams != null ? scenarioMetaParams.getMAIN_CAMPAIGN_NAME() + "_" + scenarioMetaParams.getVERSION() + "_" +
                    request.getComputed().getSegmentName() + "_1_" + currentDate : EMPTY_STRING;
            String eventName = scenarioMetaParams != null ? scenarioMetaParams.getMAIN_CAMPAIGN_NAME() + "_EVT" : EMPTY_STRING;
            String direction = scenarioMetaParams != null ? scenarioMetaParams.getDIRECTION() : "";
            String businessGroup = scenarioMetaParams != null ? scenarioMetaParams.getBUSINESS_GROUP() : "";
            String campaignObjective = scenarioMetaParams != null ? scenarioMetaParams.getCAMPAIGN_OBJECTIVE() : "";
            String campaignType = scenarioMetaParams != null ? scenarioMetaParams.getCAMPAIGN_TYPE() : "";
            String offerStartDate = currentDate;
            String offerEndDate = null;
            if(scenarioMetaParams != null && scenarioMetaParams.getEXIT_AFTER_DAY() != null) {
                LocalDate offerExpiryDate = now.plusDays(Long.parseLong(scenarioMetaParams.getEXIT_AFTER_DAY()));
                offerEndDate = offerExpiryDate.format(formatter);
            }
            String mainCampaignName = scenarioMetaParams != null ? scenarioMetaParams.getMAIN_CAMPAIGN_NAME() : "";

            String message = "";
            switch (request.getComputed().getLanguagePreference()){
                case ENG: message = request.getComputed().getTextEng(); break;
                case AR: message = request.getComputed().getTextAr(); break;
                case ENG_AR: message = request.getComputed().getTextMixed(); break;
            }
            ps.setString(1, request.getBase().getActorId());
            ps.setString(2, contractId);
            ps.setString(3, request.getComputed().getLanguagePreference());
            ps.setString(4, message);
            ps.setString(5,request.getComputed().getSenderId());
            if(request.getComputed().isControlGroup()){
                ps.setString(6, CONTROL_GROUP);
            }else{
                ps.setString(6, String.valueOf(request.getComputed().isTestModeEnabled()));
            }
            ps.setString(7, String.valueOf(request.getComputed().isFlashSmsEnabled()));
            ps.setString(8, camId);
            ps.setString(9, request.getBase().getScenarioName());
            ps.setString(10, request.getComputed().getSegmentName());
            ps.setString(11, request.getComputed().getTransactionId());
            ps.setString(12, request.getComputed().getTreatmentName());
            ps.setString(13, "1");
            ps.setString(14, request.getComputed().getOfferName());
            ps.setString(15, "1");
            ps.setString(16, usecaseTreatmentCode);
            ps.setString(17, request.getBase().getCommunication_uuid());
            ps.setString(18, request.getComputed().getTreatmentCode());
            ps.setString(19, request.getBase().getCommunication_name());
            ps.setString(20, request.getComputed().getMessageType());
            ps.setString(21, customerType);
            ps.setLong(22, request.getMessageId() == null || request.getMessageId().isEmpty() ? 0L : Long.parseLong(request.getMessageId()));
            ps.setString(23, eventName);
            ps.setString(24, request.getComputed().getMappingName());
            ps.setDouble(25, request.getComputed().getPrice() == null || request.getComputed().getPrice().isEmpty() ? 0 : Double.parseDouble(request.getComputed().getPrice()));
            ps.setString(26, request.getComputed().getOfferCategory());
            ps.setInt(27, Integer.parseInt(tgCgFlag));
            ps.setString(28, direction);
            ps.setString(29, businessGroup);
            ps.setString(30, businessSubGroup);
            ps.setString(31, campaignObjective);
            ps.setString(32, campaignType);
            ps.setString(33, request.getComputed().getOfferType());
            //SET NULL IF DATE IS NOT VALID
            ps.setDate(34, java.sql.Date.valueOf(LocalDate.parse(offerStartDate, formatter)));
            //SET NULL IF DATE IS NOT VALID
            if(offerEndDate != null) {
                ps.setDate(35, java.sql.Date.valueOf(LocalDate.parse(offerEndDate, formatter)));
            }else{
                ps.setNull(35, Types.DATE);
            }
            ps.setString(36, altContactNumber);
            ps.setString(37, mainCampaignName);
            if(request.getComputed().isControlGroup()){
                ps.setString(38, CONTROL_GROUP);
            }else{
                ps.setNull(38, java.sql.Types.VARCHAR);
            }
            ps.setString(39, QUOTA_EXCEEDED.equals(status) ? REJECTED : status);
            if (SUCCESS.equals(status)) {
                // Başarılıysa, her iki neden kolonu da null olmalı.
//                ps.setNull(40, java.sql.Types.VARCHAR); // REASON
//                ps.setNull(41, java.sql.Types.VARCHAR); // REASON_DETAIL
                ps.setNull(40, java.sql.Types.VARCHAR); // REASON

                // --- YENİ VE NİHAİ MANTIK BAŞLIYOR ---

                // 1. Orijinal isteğin parametre listesini al.
                List<com.evam.marketing.communication.template.service.client.model.Parameter> originalParams =
                        request.getOriginalRequest().getParameters();

                String applyContactPolicyValue = "TRUE"; // Varsayılan olarak TRUE kabul et

                // 2. Parametre listesini döngüyle kontrol et ve 'applyContactPolicy' değerini bul.
                if (originalParams != null) {
                    for (com.evam.marketing.communication.template.service.client.model.Parameter p : originalParams) {
                        if ("applyContactPolicy".equals(p.getName())) {
                            applyContactPolicyValue = p.getValue();
                            break; // Değeri bulunca döngüden çık
                        }
                    }
                }

                // 3. Bulunan değere göre REASON_DETAIL kolonunu ayarla.
                if ("FALSE".equalsIgnoreCase(applyContactPolicyValue)) {
                    ps.setString(41, "SKIP_CP");
                } else {
                    ps.setNull(41, java.sql.Types.VARCHAR); // Diğer tüm başarılı durumlarda (TRUE veya parametre yoksa) null
                }
                // --- YENİ VE NİHAİ MANTIK BİTİYOR ---


            } else {
                // Başarısız durumlar için...
                String quotaCheck = request.getComputed().getQuotaCheck();

                if ("PROPER_TIME".equals(quotaCheck)) {

                    ps.setString(40, "PROPER_TIME");
                    // REASON_DETAIL = null (Yeni kural)
                    ps.setNull(41, java.sql.Types.VARCHAR);
                    log.debug("Proper Time case for TransactionID: {}. Setting REASON [40] to 'PROPER_TIME' and REASON_DETAIL [41] to null.",
                            request.getComputed().getTransactionId());

                } else if (request.getComputed().isControlGroup()) {
                    // KURAL: Control Group durumu (YENİ EKLENEN KONTROL)

                    ps.setString(40, CONTROL_GROUP);
                    // REASON_DETAIL = null (Yeni kural)
                    ps.setNull(41, java.sql.Types.VARCHAR);
                    log.debug("Control Group case for TransactionID: {}. Setting REASON [40] to 'CONTROL_GROUP' and REASON_DETAIL [41] to null.",
                            request.getComputed().getTransactionId());

                } else if (quotaCheck != null && !"ok".equalsIgnoreCase(quotaCheck) && !"PROPER_TIME".equals(quotaCheck)) {
                // YENİ KURAL: Sadece quotaCheck'te GERÇEK bir contact policy hatası varsa REASON'ı ayarla.
                ps.setString(40, CONTACT_POLICY);
                ps.setString(41, quotaCheck);
                log.debug("CONTACT_POLICY case for TransactionID: {}. Setting REASON [30] to 'CONTACT_POLICY' and REASON_DETAIL [31] to '{}'.",
                        request.getComputed().getTransactionId(), quotaCheck);
             }
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
            log.error("SQLException during PreparedStatement setup for TransactionID: {}. Error: {}",
                    request.getComputed().getTransactionId(), e.getMessage(), e);
        }

    }
}

