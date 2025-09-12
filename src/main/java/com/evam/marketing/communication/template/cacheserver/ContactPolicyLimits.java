package com.evam.marketing.communication.template.cacheserver;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ContactPolicyLimits {
    private Integer dailyLimit;
    private Integer weeklyLimit;
    private Integer monthlyLimit;
    private Integer dailyCount;
    private Integer weeklySum;
    private Integer monthlySum;
}
