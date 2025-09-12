package com.evam.marketing.communication.template.controller.health;

import com.evam.marketing.communication.template.controller.health.model.response.HealthResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * Created by cemserit on 26.02.2021.
 */
@RestController
@RequestMapping(HealthControllerMapping.HEALTH_PATH)
public class HealthController {
    @GetMapping
    public Mono<HealthResponse> healthCheck() {
        return Mono.just(HealthResponse.builder()
                .status("UP")
                .build()
        );
    }
}
