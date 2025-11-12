package com.cloudrangers.cloudpilotworker.config;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class JacksonConfig {

    @Bean
    @Primary
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        // snake_case ↔ camelCase 자동 변환
        // 백엔드가 snake_case를 사용하는 경우를 대비
        mapper.setPropertyNamingStrategy(PropertyNamingStrategies.LOWER_CAMEL_CASE);

        // 알 수 없는 프로퍼티 무시 (백엔드에 추가 필드가 있어도 OK)
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        // null 값 무시
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        // 날짜 형식 설정
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // 빈 Bean 직렬화 실패 방지
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        // 빈 문자열을 null로 처리
        mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);

        return mapper;
    }
}