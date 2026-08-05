package com.fintech.notifications.api.service;

import com.fintech.notifications.api.exception.TemplateResolutionException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class TemplateResolver {

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{\\{\\s*([\\w.]+)\\s*}}");

    public String resolve(String template, Map<String, Object> params) {
        Map<String, Object> safeParams = params != null ? params : Map.of();

        Set<String> missing = new LinkedHashSet<>();
        Matcher matcher = PLACEHOLDER.matcher(template);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String key = matcher.group(1);
            Object value = safeParams.get(key);
            if (value == null) {
                missing.add(key);
                matcher.appendReplacement(result, "");
            } else {
                matcher.appendReplacement(result, Matcher.quoteReplacement(String.valueOf(value)));
            }
        }
        matcher.appendTail(result);

        if (!missing.isEmpty()) {
            throw new TemplateResolutionException(new ArrayList<>(missing));
        }
        return result.toString();
    }
}
