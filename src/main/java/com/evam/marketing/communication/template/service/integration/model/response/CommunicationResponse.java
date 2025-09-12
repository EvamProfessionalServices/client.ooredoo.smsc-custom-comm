package com.evam.marketing.communication.template.service.integration.model.response;

import com.evam.marketing.communication.template.repository.status.model.CustomCommunicationStatus;
import com.evam.marketing.communication.template.repository.status.model.StatusType;
import com.evam.marketing.communication.template.service.event.model.CommunicationFailEvent;
import com.evam.marketing.communication.template.service.event.model.CommunicationResponseEvent;
import com.evam.marketing.communication.template.service.event.model.CommunicationSuccessEvent;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Created by cemserit on 13.04.2021.
 */
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CommunicationResponse {

    private boolean success;
    private String providerResponseId;
    private String reason;
    private String message;
    @Setter
    private String provider;
    @Setter
    private String communicationUUID;
    @Setter
    private String communicationCode;
    @Setter
    private String scenario;
    @Setter
    private String actorId;

    public CustomCommunicationStatus toCommunicationStatus() {
        String substringReason = Objects.isNull(reason) ? null :
                reason.substring(0, Math.min(reason.length(), 250));
        StatusType statusType = success ? StatusType.SENT : StatusType.FAILED;

        return CustomCommunicationStatus.builder()
                .communicationUUID(communicationUUID)
                .status(statusType.name())
                .provider(provider)
                .providerResultId(providerResponseId)
                .reason(substringReason)
                .description(message)
                .statusUpdateTime(Timestamp.from(Instant.now()))
                .build();
    }

    public CommunicationResponseEvent toEvent() {
        return success ? toSuccessEvent() : toFailEvent();
    }

    private CommunicationResponseEvent toSuccessEvent() {
        return CommunicationSuccessEvent.builder()
                .communicationCode(communicationCode)
                .communicationUUID(communicationUUID)
                .scenario(scenario)
                .actorId(actorId)
                .message(message)
                .build();
    }

    private CommunicationResponseEvent toFailEvent() {
        return CommunicationFailEvent.builder()
                .communicationCode(communicationCode)
                .communicationUUID(communicationUUID)
                .scenario(scenario)
                .actorId(actorId)
                .reason(reason)
                .message(message)
                .build();
    }
}
