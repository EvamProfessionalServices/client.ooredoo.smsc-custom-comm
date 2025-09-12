package com.evam.marketing.communication.template.service.integration;

import com.evam.marketing.communication.template.service.client.model.CustomCommunicationRequest;
import com.evam.marketing.communication.template.service.client.model.Parameter;
import lombok.Builder;
import lombok.Data;

import java.time.ZonedDateTime;
import java.util.List;

/**
 * Created by okkes on 22.02.2024 in Islamabad office.
 */
@Data
@Builder
public class ServiceRequest {

    private CustomCommunicationRequest originalRequest;

    private Base base;
    private Computed computed;
    private Response response;
    private String messageId;

    @Data
    @Builder
    static
    class Base {
        private String actorId;
        private String communication_uuid;
        private String scenarioName;
        private String communication_code;
        private String communication_name;
    }

    @Data
    @Builder
    static
    class Computed {
        private boolean testModeEnabled;
        private boolean controlGroup;
        private String message;
        private String messageId;
        private String to;
        private String senderId;
        private String languagePreference;
        private String textEng;
        private String textAr;
        private String textMixed;
        private boolean flashSmsEnabled;
        private String segmentName;
        private String camId;
        private String messageType;
        private String contractId;
        private String transactionId;
        private String treatmentName;
        private String treatmentId;
        private String offerName;
        private String offerId;
        private String usercaseTreatmentCode;
        private String treatmentCode;
        private String customerType;
        private String eventName;
        private String mappingName;
        private String price;
        private String offerCategory;
        private String tgCgFlag;
        private String direction;
        private String businessGroup;
        private String businessSubGroup;
        private String campaignObjective;
        private String campaignType;
        private String offerType;
        private String offerStartDate;
        private String offerEndDate;
        private String altContactNumber;
        private String startHour;
        private String endHour;
        private String mainCampaignName;
        private ZonedDateTime timestamp;
        private String quotaCheck;
    }


    @Data
    @Builder
    static
    class Response {
        private String responseBody;
        private String responseCode;
        private String controlId;
        private String status;
    }

}
