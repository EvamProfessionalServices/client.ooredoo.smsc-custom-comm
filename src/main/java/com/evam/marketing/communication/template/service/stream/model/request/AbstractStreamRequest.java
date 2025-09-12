package com.evam.marketing.communication.template.service.stream.model.request;

import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Collections;
import java.util.Map;

/**
 * Created by cemserit on 15.04.2021.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public abstract class AbstractStreamRequest implements StreamRequest {
    private String name;
    @NotNull
    private String code;
    @NotNull
    private String uuid;
    @NotNull
    private String scenario;
    @NotNull
    private int scenarioVersion;
    @NotNull
    private String actorId;
    private String type;
    private String messageType;

    @Override
    public Map<String, Object> getResourceVariables() {
        return Collections.emptyMap();
    }

    @Override
    public boolean hasResource() {
        return false;
    }
}
