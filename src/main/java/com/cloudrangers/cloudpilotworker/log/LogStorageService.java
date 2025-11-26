package com.cloudrangers.cloudpilotworker.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * 로그 로컬 저장 및 S3 업로드 서비스
 *
 * 파일명 정책:
 *  - 정제 로그(refined):  {timestamp}_{jobId}-refined.json
 *  - 원본 로그(raw):      {timestamp}_{jobId}-raw.log.gz
 *
 * 동작 정책:
 *  - raw 로그: 모든 시도마다 저장 + (옵션) S3 업로드 (RAW 버킷)
 *  - refined 로그:
 *      - 마지막 시도(isFinalAttempt=true)일 때만 로컬 + S3(refined 버킷)에 업로드
 *      - 같은 jobId의 이전 refined 로컬 파일들은 삭제 → job당 최신 1개만 유지
 *      - S3에는 “한 번만” putObject → 람다도 job당 1번만 트리거
 */
@Service
@Slf4j
public class LogStorageService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired(required = false)
    private S3Client s3Client;

    @Value("${log.storage.base-dir:./logs}")
    private String baseDir;

    @Value("${log.storage.enabled:true}")
    private boolean storageEnabled;

    @Value("${log.storage.compress-raw:true}")
    private boolean compressRaw;

    @Value("${log.s3.enabled:false}")
    private boolean s3Enabled;

    @Value("${log.s3.bucket:cloudpilot-refined-log}")
    private String s3Bucket;

    @Value("${log.s3.raw-bucket:cloudpilot-raw-log}")
    private String s3RawBucket;

    /**
     * 로그를 로컬 파일 시스템에 저장하고 S3에 업로드
     *
     * @param jobId          작업 ID
     * @param jobType        terraform / ansible-provision / ansible-package 등
     * @param refinedLog     정제된 로그 문자열 (라인 여러 개)
     * @param rawLog         원본 로그 문자열
     * @param isFinalAttempt 마지막 시도 여부 (true: 성공 or 3번 실패 후)
     */
    public void saveLogsToLocal(String jobId,
                                String jobType,
                                String refinedLog,
                                String rawLog,
                                boolean isFinalAttempt) {
        if (!storageEnabled) {
            log.debug("Log storage is disabled, skipping file save");
            return;
        }

        try {
            // 디렉토리 경로 생성
            Path logDir = Paths.get(baseDir, jobType);
            Files.createDirectories(logDir);

            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH:mm:ss");
            String timestamp = now.format(dateTimeFormatter);

            // 파일명: 앞에 타임스탬프 + 뒤에 jobId
            String refinedFileName = String.format("%s_%s-refined.json", timestamp, jobId);
            String rawFileName = String.format("%s_%s-raw.log.gz", timestamp, jobId);

            // ISO 8601 타임스탬프 (로그 내용에 포함)
            String isoTimestamp = java.time.Instant.now().toString();

            // --------------------------------------------------
            // 1. 정제된 로그: 마지막 시도일 때만 생성 + S3 업로드
            //    → S3에는 한 번만 putObject 되도록 보장하고,
            //      로컬에서는 같은 jobId의 이전 파일들을 삭제한다.
            // --------------------------------------------------
            if (isFinalAttempt) {
                if (refinedLog != null && !refinedLog.isEmpty()) {
                    log.info("Final attempt - saving refined log for jobId={}, jobType={}", jobId, jobType);

                    // 같은 jobId 의 이전 refined 로컬 파일 제거
                    try (var stream = Files.list(logDir)) {
                        stream.filter(path ->
                                        path.getFileName().toString().endsWith("_" + jobId + "-refined.json"))
                                .forEach(p -> {
                                    try {
                                        Files.deleteIfExists(p);
                                        log.info("Deleted old refined log: {}", p);
                                    } catch (IOException ex) {
                                        log.warn("Failed to delete old refined log: {}", p, ex);
                                    }
                                });
                    }

                    Path refinedPath = logDir.resolve(refinedFileName);

                    Map<String, Object> logJson = new HashMap<>();
                    logJson.put("jobId", jobId);
                    logJson.put("timestamp", isoTimestamp);
                    logJson.put("jobType", jobType);
                    logJson.put("finalAttempt", true);
                    logJson.put("log", refinedLog);

                    String jsonContent = objectMapper
                            .writerWithDefaultPrettyPrinter()
                            .writeValueAsString(logJson);

                    Files.writeString(refinedPath, jsonContent, StandardCharsets.UTF_8);
                    log.info("Refined log saved (JSON): {}", refinedPath.toAbsolutePath());

                    // ✅ 이 시점에서만 S3 refined 버킷에 업로드
                    if (s3Enabled && s3Client != null) {
                        uploadToS3(jobType, refinedFileName, jsonContent);
                    }
                } else {
                    log.info("Final attempt but refinedLog is null/empty for jobId={}, jobType={}", jobId, jobType);
                }
            } else {
                log.debug("Not final attempt - skipping refined log for jobId={}, jobType={}", jobId, jobType);
            }

            // --------------------------------------------------
            // 2. 원본 로그: 매 시도마다 저장 + S3 RAW 버킷 업로드
            //    → 여기에 람다 이벤트 안 걸면 상관 없음
            // --------------------------------------------------
            if (rawLog != null && !rawLog.isEmpty()) {
                if (compressRaw) {
                    Path rawPath = logDir.resolve(rawFileName);
                    saveCompressed(rawPath, rawLog);
                    log.info("Raw log saved (compressed): {}", rawPath.toAbsolutePath());

                    if (s3Enabled && s3Client != null) {
                        uploadRawToS3(jobType, rawFileName, rawPath);
                    }
                } else {
                    String rawPlainName = rawFileName.replace(".gz", "");
                    Path rawPath = logDir.resolve(rawPlainName);
                    Files.writeString(rawPath, rawLog, StandardCharsets.UTF_8);
                    log.info("Raw log saved: {}", rawPath.toAbsolutePath());

                    if (s3Enabled && s3Client != null) {
                        uploadRawToS3(jobType, rawPlainName, rawPath);
                    }
                }
            } else {
                log.debug("Raw log is null/empty for jobId={}, jobType={} (skipping raw file)", jobId, jobType);
            }

        } catch (Exception e) {
            log.error("Failed to save logs to local filesystem for jobId: {}", jobId, e);
        }
    }

    /**
     * S3에 파일 업로드 (정제된 로그)
     */
    private void uploadToS3(String jobType, String fileName, String content) {
        try {
            String s3Key = String.format("%s/%s", jobType, fileName);

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(s3Bucket)
                    .key(s3Key)
                    .contentType("application/json")
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromString(content));

            log.info("Uploaded refined log to S3: s3://{}/{}", s3Bucket, s3Key);

        } catch (Exception e) {
            log.error("Failed to upload refined log to S3: {}/{}", jobType, fileName, e);
        }
    }

    /**
     * S3에 원본 로그 파일 업로드 (RAW 버킷)
     */
    private void uploadRawToS3(String jobType, String fileName, Path filePath) {
        try {
            String s3Key = String.format("%s/%s", jobType, fileName);

            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(s3RawBucket)
                    .key(s3Key)
                    .contentType(fileName.endsWith(".gz") ? "application/gzip" : "text/plain")
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromFile(filePath));

            log.info("Uploaded raw log to S3: s3://{}/{}", s3RawBucket, s3Key);

        } catch (Exception e) {
            log.error("Failed to upload raw log to S3: {}/{}", jobType, fileName, e);
        }
    }

    /**
     * 텍스트를 GZIP으로 압축해서 저장
     */
    private void saveCompressed(Path path, String content) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             GZIPOutputStream gzip = new GZIPOutputStream(fos)) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * 로컬에서 정제된 로그 내용("log" 필드)만 읽기 (AI 전달용)
     */
    public String getRefinedLog(String jobId, String jobType) {
        if (!storageEnabled) {
            log.warn("Log storage is disabled, cannot retrieve log");
            return null;
        }

        try {
            Path logDir = Paths.get(baseDir, jobType);

            if (!Files.exists(logDir)) {
                log.warn("Log directory not found: {}", logDir);
                return null;
            }

            // timestamp_jobId-refined.json 중 가장 최근 것
            Path refinedPath = Files.list(logDir)
                    .filter(path -> path.getFileName().toString().endsWith("_" + jobId + "-refined.json"))
                    .max((p1, p2) -> p1.getFileName().toString().compareTo(p2.getFileName().toString()))
                    .orElse(null);

            if (refinedPath != null && Files.exists(refinedPath)) {
                String jsonContent = Files.readString(refinedPath, StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                Map<String, Object> logJson = objectMapper.readValue(jsonContent, Map.class);
                return (String) logJson.get("log");
            } else {
                log.warn("Refined log not found for jobId: {} in {}", jobId, logDir);
                return null;
            }

        } catch (Exception e) {
            log.error("Failed to retrieve log from local filesystem for jobId: {}", jobId, e);
            return null;
        }
    }

    /**
     * JSON 형식의 정제 로그 파일 전체를 읽기
     */
    public String getRefinedLogJson(String jobId, String jobType) {
        if (!storageEnabled) {
            log.warn("Log storage is disabled, cannot retrieve log");
            return null;
        }

        try {
            Path logDir = Paths.get(baseDir, jobType);

            if (!Files.exists(logDir)) {
                log.warn("Log directory not found: {}", logDir);
                return null;
            }

            Path refinedPath = Files.list(logDir)
                    .filter(path -> path.getFileName().toString().endsWith("_" + jobId + "-refined.json"))
                    .max((p1, p2) -> p1.getFileName().toString().compareTo(p2.getFileName().toString()))
                    .orElse(null);

            if (refinedPath != null && Files.exists(refinedPath)) {
                return Files.readString(refinedPath, StandardCharsets.UTF_8);
            } else {
                log.warn("Refined log not found for jobId: {} in {}", jobId, logDir);
                return null;
            }

        } catch (Exception e) {
            log.error("Failed to retrieve JSON log from local filesystem for jobId: {}", jobId, e);
            return null;
        }
    }

    /**
     * 로그 파일 경로 반환
     *
     * @param suffix "-refined.json" 또는 "-raw.log.gz"
     * @return 가장 최근 파일의 절대 경로
     */
    public String getLogPath(String jobId, String jobType, String suffix) {
        try {
            Path logDir = Paths.get(baseDir, jobType);

            if (!Files.exists(logDir)) {
                return null;
            }

            String pattern = "_" + jobId + suffix;
            final String finalPattern = pattern;

            Path logPath = Files.list(logDir)
                    .filter(path -> path.getFileName().toString().endsWith(finalPattern))
                    .max((p1, p2) -> p1.getFileName().toString().compareTo(p2.getFileName().toString()))
                    .orElse(null);

            return logPath != null ? logPath.toAbsolutePath().toString() : null;

        } catch (Exception e) {
            log.error("Failed to get log path for jobId: {}", jobId, e);
            return null;
        }
    }

    /**
     * S3 URL 반환 (정제된 로그)
     *  - s3://{refinedBucket}/{jobType}/{timestamp_jobId-refined.json}
     *    (마지막 시도 기준)
     */
    public String getS3Url(String jobId, String jobType) {
        if (!s3Enabled) {
            return null;
        }

        String localPath = getLogPath(jobId, jobType, "-refined.json");
        if (localPath == null) {
            return null;
        }

        Path path = Paths.get(localPath);
        String fileName = path.getFileName().toString();

        String s3Key = String.format("%s/%s", jobType, fileName);
        return String.format("s3://%s/%s", s3Bucket, s3Key);
    }

    /**
     * Raw 로그 S3 URL 반환
     *  - s3://{rawBucket}/{jobType}/{timestamp_jobId-raw.log.gz}
     *    (가장 최근 시도 기준)
     */
    public String getRawS3Url(String jobId, String jobType) {
        if (!s3Enabled) {
            return null;
        }

        String localPath = getLogPath(jobId, jobType, "-raw.log.gz");
        if (localPath == null) {
            return null;
        }

        Path path = Paths.get(localPath);
        String fileName = path.getFileName().toString();

        String s3Key = String.format("%s/%s", jobType, fileName);
        return String.format("s3://%s/%s", s3RawBucket, s3Key);
    }
}
