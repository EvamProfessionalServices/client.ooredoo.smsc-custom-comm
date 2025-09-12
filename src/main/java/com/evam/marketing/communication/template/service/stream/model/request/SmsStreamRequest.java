package com.evam.marketing.communication.template.service.stream.model.request;

import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;


/**
 * Created by cemserit on 20.04.2021.
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
public class SmsStreamRequest extends AbstractStreamRequest {
    @NotNull
    private String from;
    @NotNull
    private String to;
    @NotNull
    private String message;
}
