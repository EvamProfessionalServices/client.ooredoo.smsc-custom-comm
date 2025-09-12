package com.evam.marketing.communication.template.utils;

import com.evam.marketing.communication.template.service.stream.model.request.StreamRequest;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.springframework.lang.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Created by cemserit on 16.02.2021.
 */
@Slf4j
@UtilityClass
public final class ResourceTemplateUtils {
    private static final String EMPTY = "";
    private static final char EVAM_ESCAPE_CHARACTER = '|';

    @Nullable
    public static String enrichContent(StreamRequest streamRequest, String rawContent) {
        Map<String, Object> variables = streamRequest.getResourceVariables();

        if (Objects.isNull(variables) || Objects.isNull(rawContent)) {
            log.debug("Skipped to replace because variable [{}] or raw content [{}] is null!", variables, rawContent);
            return rawContent;
        }

        return replaceVariables(rawContent, variables);
    }

    private static String replaceVariables(String rawContent, Map<String, Object> variables) {
        String replacedContent = rawContent;
        for (Map.Entry<String, Object> e : variables.entrySet()) {
            String valueStr = Objects.isNull(e.getValue()) ? EMPTY : String.valueOf(e.getValue());
            replacedContent = replacedContent.replaceAll(EVAM_ESCAPE_CHARACTER + Pattern.quote(e.getKey()) + EVAM_ESCAPE_CHARACTER, valueStr);
        }

        log.debug("Content has been replaced successfully. {}", replacedContent);
        return replacedContent;
    }
}
