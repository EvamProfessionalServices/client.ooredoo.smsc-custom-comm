package com.evam.marketing.communication.template.repository.template.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.SequenceGenerator;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.*;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by cemserit on 15.02.2021.
 */
@Entity
@Table(name = "int_resource_template", uniqueConstraints = @UniqueConstraint(
        columnNames = {"code", "scenario_name", "scenario_version"})
)
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ResourceTemplate implements Serializable {
    @GeneratedValue(strategy = GenerationType.AUTO, generator = "int_resource_template_seq")
    @SequenceGenerator(name = "int_resource_template_seq", sequenceName = "int_resource_template_seq",
        allocationSize = 1)
    @Id
    private Long id;
    @Column(name = "code")
    private String communicationCode;
    @Column(name = "scenario_name")
    private String scenarioName;
    @Column(name = "scenario_version")
    private int scenarioVersion;
    @Lob
    @Column(name = "content", columnDefinition = "TEXT")
    private String content;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceTemplate that = (ResourceTemplate) o;
        return scenarioVersion == that.scenarioVersion
                && Objects.equals(communicationCode, that.communicationCode)
                && Objects.equals(scenarioName, that.scenarioName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(communicationCode, scenarioName, scenarioVersion);
    }
}
