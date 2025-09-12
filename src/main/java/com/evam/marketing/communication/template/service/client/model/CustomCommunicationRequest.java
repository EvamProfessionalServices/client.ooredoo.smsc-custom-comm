package com.evam.marketing.communication.template.service.client.model;

import com.evam.marketing.communication.template.service.integration.model.request.AbstractCommunicationRequest;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * Created by cemserit on 4.04.2021.
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class CustomCommunicationRequest extends AbstractCommunicationRequest {

    @ToString.Exclude
    private List<Parameter> parameters;
    private String quotaCheck;
}
