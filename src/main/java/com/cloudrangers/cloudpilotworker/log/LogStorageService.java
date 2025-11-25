package com.cloudrangers.cloudpilotworker.log;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

/**
 * 로그 로컬 저장 서비스
 */
@Service
@Slf4j
public class LogStorageService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${log.storage.base-dir:./logs}")
    private String baseDir;

    @Value("${log.storage.enabled:true}")
    private boolean storageEnabled;

    @Value("${log.storage.compress-raw:true}")
    private boolean compressRaw;

    /**
     * 로그를 로컬 파일 시스템에 저장
     * @param jobId 작업 ID
     * @param jobType terraform / ansible-provision / ansible-package
     * @param refinedLog 정제된 로그
     * @param rawLog 원본 로그
     */
    public void saveLogsToLocal(String jobId, String jobType, String refinedLog, String rawLog) {
        if (!storageEnabled) {
            log.debug("Log storage is disabled, skipping file save");
            return;
        }

        try {
            LocalDate now = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            String datePath = now.format(formatter);

            // 디렉토리 경로 생성
            Path logDir = Paths.get(baseDir, jobType, datePath);
            Files.createDirectories(logDir);

            // 1. 정제된 로그를 JSON 형식으로 저장 (AI용)
            Path refinedPath = logDir.resolve(jobId + "-refined.json");
            Map<String, String> logJson = new HashMap<>();
            logJson.put("log", refinedLog);

            String jsonContent = objectMapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(logJson);
            Files.writeString(refinedPath, jsonContent, StandardCharsets.UTF_8);
            log.info("Refined log saved (JSON): {}", refinedPath.toAbsolutePath());

            // 2. 원본 로그 저장 (디버깅용)
            if (rawLog != null && !rawLog.isEmpty()) {
                if (compressRaw) {
                    Path rawPath = logDir.resolve(jobId + "-raw.log.gz");
                    saveCompressed(rawPath, rawLog);
                    log.info("Raw log saved (compressed): {}", rawPath.toAbsolutePath());
                } else {
                    Path rawPath = logDir.resolve(jobId + "-raw.log");
                    Files.writeString(rawPath, rawLog, StandardCharsets.UTF_8);
                    log.info("Raw log saved: {}", rawPath.toAbsolutePath());
                }
            }

        } catch (Exception e) {
            log.error("Failed to save logs to local filesystem for jobId: {}", jobId, e);
            // 로그 저장 실패는 전체 프로세스를 중단시키지 않음
        }
    }

    /**
     * 텍스트를 압축하여 저장
     */
    private void saveCompressed(Path path, String content) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             GZIPOutputStream gzip = new GZIPOutputStream(fos)) {
            gzip.write(content.getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * 로컬에서 정제된 로그 읽기 (AI 전달용)
     * @return JSON 파일에서 "log" 키의 값을 반환
     */
    public String getRefinedLog(String jobId, String jobType) {
        if (!storageEnabled) {
            log.warn("Log storage is disabled, cannot retrieve log");
            return null;
        }

        try {
            LocalDate now = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            String datePath = now.format(formatter);

            Path refinedPath = Paths.get(baseDir, jobType, datePath, jobId + "-refined.json");

            if (Files.exists(refinedPath)) {
                String jsonContent = Files.readString(refinedPath, StandardCharsets.UTF_8);
                @SuppressWarnings("unchecked")
                Map<String, String> logJson = objectMapper.readValue(jsonContent, Map.class);
                return logJson.get("log");
            } else {
                log.warn("Refined log not found: {}", refinedPath);
                return null;
            }

        } catch (Exception e) {
            log.error("Failed to retrieve log from local filesystem for jobId: {}", jobId, e);
            return null;
        }
    }

    /**
     * JSON 형식의 로그 파일 전체를 읽기 (JSON 그대로)
     */
    public String getRefinedLogJson(String jobId, String jobType) {
        if (!storageEnabled) {
            log.warn("Log storage is disabled, cannot retrieve log");
            return null;
        }

        try {
            LocalDate now = LocalDate.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            String datePath = now.format(formatter);

            Path refinedPath = Paths.get(baseDir, jobType, datePath, jobId + "-refined.json");

            if (Files.exists(refinedPath)) {
                return Files.readString(refinedPath, StandardCharsets.UTF_8);
            } else {
                log.warn("Refined log not found: {}", refinedPath);
                return null;
            }

        } catch (Exception e) {
            log.error("Failed to retrieve JSON log from local filesystem for jobId: {}", jobId, e);
            return null;
        }
    }

    /**
     * 로그 파일 경로 반환 (외부에서 직접 접근용)
     */
    public String getLogPath(String jobId, String jobType, String suffix) {
        LocalDate now = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        String datePath = now.format(formatter);

        // refined 로그는 .json 확장자
        if (suffix.equals("-refined.log")) {
            suffix = "-refined.json";
        }

        Path logPath = Paths.get(baseDir, jobType, datePath, jobId + suffix);
        return logPath.toAbsolutePath().toString();
    }
}