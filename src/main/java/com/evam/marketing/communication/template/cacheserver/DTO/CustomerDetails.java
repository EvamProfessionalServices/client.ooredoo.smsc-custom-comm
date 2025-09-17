package com.evam.marketing.communication.template.cacheserver.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CustomerDetails {
    private String msisdnalt0;
    private String contractid;
    private String customertype;
    private String ucgflag;
    private String msisdn_cont_base_ls;
}
