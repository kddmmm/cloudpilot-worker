package com.cloudrangers.cloudpilotworker.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
// 🔥 추가
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // 🔥 Java Time 모듈 등록 (OffsetDateTime, LocalDateTime 등 지원)
        mapper.registerModule(new JavaTimeModule());
        // (필요하면 JDK8 모듈도: mapper.registerModule(new Jdk8Module());)

        // snake_case ↔ camelCase 자동 변환
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);

        // 알 수 없는 프로퍼티 무시
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // null 값 무시
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // 날짜를 타임스탬프 숫자가 아니라 ISO 문자열로
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // 빈 Bean 직렬화 실패 방지
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        // 빈 문자열을 null로
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);

        return mapper;
    }
}
