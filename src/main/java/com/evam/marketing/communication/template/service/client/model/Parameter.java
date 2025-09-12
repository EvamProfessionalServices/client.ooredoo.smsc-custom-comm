package com.evam.marketing.communication.template.service.client.model;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
@SuperBuilder
@ToString
public class Parameter {
    private String name;
    private String value;
}
