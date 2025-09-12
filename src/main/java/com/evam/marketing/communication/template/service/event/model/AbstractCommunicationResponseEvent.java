package com.evam.marketing.communication.template.service.event.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Created by cemserit on 2.03.2021.
 */
@Getter
@SuperBuilder
@ToString(callSuper = true)
@EqualsAndHashCode
public abstract class AbstractCommunicationResponseEvent implements CommunicationResponseEvent {

    private String name;
    private String scenario;
    private String actorId;
    private String communicationCode;
    private String message;
    @JsonIgnore
    private String communicationUUID;

    private Map<String, Object> customParameters;

    @Override
    public void addCustomParameter(String parameterKey, String parameterValue) {
        if (customParameters == null) {
            customParameters = new HashMap<>();
        }
        customParameters.put(parameterKey, parameterValue);
    }

    @Override
    public void addCustomParameter(String parameterKey, BigDecimal parameterValue) {
        if (customParameters == null) {
            customParameters = new HashMap<>();
        }
        customParameters.put(parameterKey, parameterValue);
    }

    @JsonAnyGetter
    @Override
    public Map<String, Object> getCustomParameters() {
        if (customParameters == null) {
            return Collections.emptyMap();
        }
        return customParameters;
    }
}
