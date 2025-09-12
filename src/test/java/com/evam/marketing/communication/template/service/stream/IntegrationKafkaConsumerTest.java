package com.evam.marketing.communication.template.service.stream;

import com.evam.marketing.communication.template.service.stream.model.request.CustomCommunicationStreamRequest;
import com.evam.marketing.communication.template.utils.SerializationUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import uk.co.jemos.podam.api.PodamFactory;
import uk.co.jemos.podam.api.PodamFactoryImpl;

/**
 * Created by cemserit at 19.03.2022
 */
@Slf4j
class IntegrationKafkaConsumerTest {

    private final PodamFactory factory = new PodamFactoryImpl();
    private final ObjectMapper objectMapper = SerializationUtils.generateObjectMapper();

    @Test
    void serializeStreamRequest() throws JsonProcessingException {

        CustomCommunicationStreamRequest streamRequest = factory.manufacturePojo(
                CustomCommunicationStreamRequest.class);

        String serializeRequest = objectMapper.writeValueAsString(streamRequest);
        log.info("Model: {}, serialize request: {}", streamRequest, serializeRequest);
    }
}