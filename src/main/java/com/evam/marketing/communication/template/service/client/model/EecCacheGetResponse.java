package com.evam.marketing.communication.template.service.client.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class EecCacheGetResponse {
    private Records records;
}