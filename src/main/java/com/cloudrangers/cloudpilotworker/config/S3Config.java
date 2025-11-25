package com.cloudrangers.cloudpilotworker.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * AWS S3 설정 (온프레미스 환경)
 * IAM Role 사용 불가, Access Key/Secret Key 필수
 */
@Configuration
@Slf4j
public class S3Config {

    @Value("${aws.s3.region:ap-northeast-2}")
    private String region;

    @Value("${aws.s3.access-key:}")
    private String accessKey;

    @Value("${aws.s3.secret-key:}")
    private String secretKey;

    @Value("${log.s3.enabled:false}")
    private boolean s3Enabled;

    @Bean
    public S3Client s3Client() {
        if (!s3Enabled) {
            log.info("S3 upload is disabled");
            return null;
        }

        if (accessKey.isEmpty() || secretKey.isEmpty()) {
            log.warn("S3 is enabled but AWS credentials are not configured. S3 upload will be skipped.");
            log.warn("Please set AWS_ACCESS_KEY and AWS_SECRET_KEY in .env file");
            return null;
        }

        AwsBasicCredentials credentials = AwsBasicCredentials.create(accessKey, secretKey);

        log.info("S3Client initialized with region: {}", region);

        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();
    }
}