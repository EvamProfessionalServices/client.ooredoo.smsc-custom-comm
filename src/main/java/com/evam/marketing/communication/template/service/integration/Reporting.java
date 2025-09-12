package com.evam.marketing.communication.template.service.integration;

import com.evam.marketing.communication.template.configuration.PersistentConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
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

    public Reporting(PersistentConfig persistentConfig) {
        this.persistentConfig = persistentConfig;
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
            String message = "";
            switch (request.getComputed().getLanguagePreference()){
                case ENG: message = request.getComputed().getTextEng(); break;
                case AR: message = request.getComputed().getTextAr(); break;
                case ENG_AR: message = request.getComputed().getTextMixed(); break;
            }
            ps.setString(1, request.getBase().getActorId());
            ps.setString(2, request.getComputed().getContractId());
            ps.setString(3, request.getComputed().getLanguagePreference());
            ps.setString(4, message);
            ps.setString(5,request.getComputed().getSenderId());
            if(request.getComputed().isControlGroup()){
                ps.setString(6, CONTROL_GROUP);
            }else{
                ps.setString(6, String.valueOf(request.getComputed().isTestModeEnabled()));
            }
            ps.setString(7, String.valueOf(request.getComputed().isFlashSmsEnabled()));
            ps.setString(8, request.getComputed().getCamId());
            ps.setString(9, request.getBase().getScenarioName());
            ps.setString(10, request.getComputed().getSegmentName());
            ps.setString(11, request.getComputed().getTransactionId());
            ps.setString(12, request.getComputed().getTreatmentName());
            ps.setString(13, request.getComputed().getTreatmentId());
            ps.setString(14, request.getComputed().getOfferName());
            ps.setString(15, request.getComputed().getOfferId());
            ps.setString(16, request.getComputed().getUsercaseTreatmentCode());
            ps.setString(17, request.getBase().getCommunication_uuid());
            ps.setString(18, request.getComputed().getTreatmentCode());
            ps.setString(19, request.getBase().getCommunication_name());
            ps.setString(20, request.getComputed().getMessageType());
            ps.setString(21, request.getComputed().getCustomerType());
            ps.setLong(22, request.getMessageId() == null || request.getMessageId().isEmpty() ? 0L : Long.parseLong(request.getMessageId()));
            ps.setString(23, request.getComputed().getEventName());
            ps.setString(24, request.getComputed().getMappingName());
            ps.setDouble(25, request.getComputed().getPrice() == null || request.getComputed().getPrice().isEmpty() ? 0 : Double.parseDouble(request.getComputed().getPrice()));
            ps.setString(26, request.getComputed().getOfferCategory());
            ps.setInt(27, request.getComputed().getTgCgFlag() == null || request.getComputed().getTgCgFlag().isEmpty() ? 0 : Integer.parseInt(request.getComputed().getTgCgFlag()));
            ps.setString(28, request.getComputed().getDirection());
            ps.setString(29, request.getComputed().getBusinessGroup());
            ps.setString(30, request.getComputed().getBusinessSubGroup());
            ps.setString(31, request.getComputed().getCampaignObjective());
            ps.setString(32, request.getComputed().getCampaignType());
            ps.setString(33, request.getComputed().getOfferType());
            //SET NULL IF DATE IS NOT VALID
            try {
                Timestamp startDate = Timestamp.valueOf(request.getComputed().getOfferStartDate());
            }catch (Exception e){
                request.getComputed().setOfferStartDate(null);
            }
            if(request.getComputed().getOfferStartDate() == null || request.getComputed().getOfferStartDate().isEmpty()){
                ps.setNull(34,java.sql.Types.TIMESTAMP);
            }else {
                ps.setTimestamp(34, Timestamp.valueOf(request.getComputed().getOfferStartDate()));
            }
            //SET NULL IF DATE IS NOT VALID
            try {
                Timestamp endDate = Timestamp.valueOf(request.getComputed().getOfferEndDate());
            }catch (Exception e){
                request.getComputed().setOfferEndDate(null);
            }
            if(request.getComputed().getOfferEndDate() == null || request.getComputed().getOfferEndDate().isEmpty()){
                ps.setNull(35,java.sql.Types.TIMESTAMP);
            }else {
                ps.setTimestamp(35, Timestamp.valueOf(request.getComputed().getOfferEndDate()));
            }
            ps.setString(36, request.getComputed().getAltContactNumber());
            ps.setString(37, request.getComputed().getMainCampaignName() == null ? "" : request.getComputed().getMainCampaignName());
            if(request.getComputed().isControlGroup()){
                ps.setString(38, CONTROL_GROUP);
            }else{
                ps.setNull(38, java.sql.Types.VARCHAR);
            }
            ps.setString(39, QUOTA_EXCEEDED.equals(status) ? REJECTED : status);
            if(SUCCESS.equals(status)){
                ps.setNull(40,java.sql.Types.VARCHAR);
            }else {
                ps.setString(40, request.getComputed().isControlGroup() ? CONTROL_GROUP : CONTACT_POLICY);
            }
        }catch (SQLException e){
            log.error(e.getMessage());
        }
    }
}