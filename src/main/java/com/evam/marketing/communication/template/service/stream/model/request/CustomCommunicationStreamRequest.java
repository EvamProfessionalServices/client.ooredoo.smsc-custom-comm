package com.evam.marketing.communication.template.service.stream.model.request;

import com.evam.marketing.communication.template.service.client.model.Parameter;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;

/**
 * Created by cemserit on 15.04.2021.
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class CustomCommunicationStreamRequest extends AbstractStreamRequest {
    private List<Parameter> parameters;
}
