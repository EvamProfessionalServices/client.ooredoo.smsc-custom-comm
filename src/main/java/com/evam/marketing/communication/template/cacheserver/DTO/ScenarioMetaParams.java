package com.evam.marketing.communication.template.cacheserver.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ScenarioMetaParams {
    private String MAIN_CAMPAIGN_NAME;
    private String BUSINESS_GROUP;
    private String EXIT_AFTER_DAY;
    private String DIRECTION;
    private String CAMPAIGN_OBJECTIVE;
    private String CAMPAIGN_TYPE;
    private String OFFER_LIMIT;
    private String TEST_MODE;
    private String SPRINT;
    private String VERSION;
}
