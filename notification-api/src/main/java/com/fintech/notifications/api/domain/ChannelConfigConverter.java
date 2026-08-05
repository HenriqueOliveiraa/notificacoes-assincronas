package com.fintech.notifications.api.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

@Converter
public class ChannelConfigConverter implements AttributeConverter<ChannelConfig, String> {

    private static final ObjectMapper MAPPER = JsonMapper.builder().build();

    @Override
    public String convertToDatabaseColumn(ChannelConfig attribute) {
        if (attribute == null) {
            return "{}";
        }
        try {
            return MAPPER.writeValueAsString(attribute);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao serializar config do canal", e);
        }
    }

    @Override
    public ChannelConfig convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.isBlank()) {
            return new ChannelConfig();
        }
        try {
            return MAPPER.readValue(dbData, ChannelConfig.class);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao desserializar config do canal", e);
        }
    }
}
