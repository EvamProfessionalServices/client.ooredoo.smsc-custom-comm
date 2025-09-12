package com.evam.marketing.communication.template.repository.status.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import java.io.Serializable;
import java.util.Date;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Created by cemserit on 5.03.2021.
 */
@Entity
@Table(name = "int_custom_comm_status", indexes = {
        @Index(name = "int_custom_com_status_uuid_id", columnList = "communication_uuid", unique = true)
})
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CustomCommunicationStatus implements Serializable {

    @GeneratedValue(strategy = GenerationType.AUTO, generator = "int_custom_comm_status_seq")
    @SequenceGenerator(name = "int_custom_comm_status_seq", sequenceName = "int_custom_comm_status_seq",
        allocationSize = 1)
    @Id
    private Long id;
    @Column(name = "communication_uuid", length = 50)
    private String communicationUUID;
    @Column(length = 50)
    private String provider;
    @Column(length = 50)
    private String status;
    @Column(length = 250)
    private String providerResultId;
    @Column(length = 250)
    private String reason;
    @Temporal(TemporalType.TIMESTAMP)
    private Date statusUpdateTime;
    @Column(length = 4000)
    private String description;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomCommunicationStatus that = (CustomCommunicationStatus) o;
        return Objects.equals(communicationUUID, that.communicationUUID)
                && Objects.equals(provider, that.provider) && Objects.equals(status,
                that.status) && Objects.equals(providerResultId, that.providerResultId)
                && Objects.equals(reason, that.reason) && Objects.equals(
                description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(communicationUUID, provider, status, providerResultId, reason,
                description);
    }
}

