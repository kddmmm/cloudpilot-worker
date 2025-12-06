package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.dto.ProvisionResultMessage;
import com.cloudrangers.cloudpilotworker.log.AnsibleLogContext;
import com.cloudrangers.cloudpilotworker.log.AnsibleLogRefiner;
import com.cloudrangers.cloudpilotworker.log.LogStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class AnsibleExecutor {

    private final ObjectMapper objectMapper;
    private final AnsibleLogRefiner logRefiner;
    private final LogStorageService logStorageService;
    private final RabbitTemplate rabbitTemplate;  // ⭐ 추가

    @Value("${rabbitmq.exchange.result.name}")
    private String resultExchange;

    @Value("${rabbitmq.routing-key.result}")
    private String resultRoutingKey;

    private static final String ANSIBLE_PLAYBOOK_PATH = "/etc/ansible/main_provision.yml";
    private static final String SSH_KEY_PATH = "/home/admin/.ssh/ansible_key";
    private static final String REMOTE_USER = "admin";

    public AnsibleExecutor(ObjectMapper objectMapper,
                           AnsibleLogRefiner logRefiner,
                           LogStorageService logStorageService,
                           RabbitTemplate rabbitTemplate) {  // ⭐ 추가
        this.objectMapper = objectMapper;
        this.logRefiner = logRefiner;
        this.logStorageService = logStorageService;
        this.rabbitTemplate = rabbitTemplate;
    }

    public void execute(String targetIp, ProvisionJobMessage msg, boolean isFinalAttempt) {
        log.info("🚀 [Ansible] Starting Provisioning for IP: {}", targetIp);

        StringBuilder refinedLog = new StringBuilder();
        StringBuilder rawLog = new StringBuilder();
        AnsibleLogContext context = new AnsibleLogContext();

        String jobId = msg.getJobId() != null ? String.valueOf(msg.getJobId()) :
                String.valueOf(System.currentTimeMillis());

        try {
            // 1. 설치할 패키지 목록 추출
            List<String> packages = new ArrayList<>();
            if (msg.getProperties() != null && msg.getProperties().getPackages() != null) {
                packages = msg.getProperties().getPackages();
            }

            // 2. Extra Vars 생성
            Map<String, Object> extraVars = new HashMap<>();
            extraVars.put("target_packages", packages);
            String extraVarsJson = objectMapper.writeValueAsString(extraVars);

            // 3. 명령어 조립
            List<String> command = new ArrayList<>();
            command.add("ansible-playbook");
            command.add("-i");
            command.add(targetIp + ",");
            command.add("--private-key");
            command.add(SSH_KEY_PATH);
            command.add("-u");
            command.add(REMOTE_USER);
            command.add("-e");
            command.add(extraVarsJson);
            command.add(ANSIBLE_PLAYBOOK_PATH);

            log.info("[Ansible] Command: {}", String.join(" ", command));

            // 4. 프로세스 실행
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            pb.environment().put("ANSIBLE_HOST_KEY_CHECKING", "False");

            Process process = pb.start();

            // 5. 로그 실시간 출력 및 RabbitMQ 전송
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // 원본 로그는 파일용 버퍼에 저장
                    rawLog.append(line).append('\n');

                    // 로그 정제
                    String refinedLine = logRefiner.refineLine(line, context);

                    // 콘솔 출력
                    log.info("[Ansible-Log] {}", line);

                    // ⭐ 정제된 로그만 RabbitMQ로 전송
                    if (refinedLine != null) {
                        refinedLog.append(refinedLine).append('\n');
                        log.info("[Ansible-refined] {}", refinedLine);

                        // RabbitMQ로 LOG 이벤트 전송
                        if (context.isInError()) {
                            sendErrorLogEvent(jobId, "ansible_provision", refinedLine);
                        } else {
                            sendLogEvent(jobId, "ansible_provision", refinedLine);
                        }
                    }
                }
            }

            // 6. 종료 대기
            boolean finished = process.waitFor(20, TimeUnit.MINUTES);
            if (!finished) {
                process.destroyForcibly();
                String timeoutMsg = "Ansible execution timed out.";
                sendErrorLogEvent(jobId, "ansible_provision", timeoutMsg);
                throw new RuntimeException(timeoutMsg);
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                String errorMsg = "Ansible execution failed with exit code: " + exitCode;
                sendErrorLogEvent(jobId, "ansible_provision", errorMsg);
                throw new RuntimeException(errorMsg);
            }

            // ⭐ 최종 완료 로그 전송
            String completionMsg = "✅ [Ansible] Provisioning Completed Successfully for IP: " + targetIp;
            sendLogEvent(jobId, "ansible_provision", completionMsg);
            log.info(completionMsg);

        } catch (Exception e) {
            log.error("❌ [Ansible] Execution Error", e);
            sendErrorLogEvent(jobId, "ansible_provision",
                    "Ansible Execution Failed: " + e.getMessage());
            throw new RuntimeException("Ansible Execution Failed", e);
        } finally {
            try {
                logStorageService.saveLogsToLocal(jobId, "ansible-provision",
                        refinedLog.toString(), rawLog.toString(), isFinalAttempt);
            } catch (Exception e) {
                log.error("Failed to save Ansible logs to local filesystem for jobId: {}", jobId, e);
            }
        }
    }

    // ⭐ LOG 이벤트 전송 메서드 추가
    private void sendLogEvent(String jobId, String step, String line) {
        if (jobId == null) return;

        try {
            ProvisionResultMessage msg = new ProvisionResultMessage();
            msg.setJobId(jobId);
            msg.setEventType(ProvisionResultMessage.EventType.LOG);
            msg.setStatus("LOG");
            msg.setStep(step);
            msg.setMessage(line);
            msg.setTimestamp(OffsetDateTime.now());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, msg, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                return m;
            });

            log.debug("📤 Sent Ansible LOG event: jobId={}, line={}", jobId,
                    line.length() > 100 ? line.substring(0, 100) + "..." : line);

        } catch (Exception e) {
            log.warn("[AnsibleExecutor] Failed to send LOG event for jobId={}: {}",
                    jobId, e.getMessage());
        }
    }

    // ⭐ ERROR 이벤트 전송 메서드 추가
    private void sendErrorLogEvent(String jobId, String step, String line) {
        if (jobId == null) return;

        try {
            ProvisionResultMessage msg = new ProvisionResultMessage();
            msg.setJobId(jobId);
            msg.setEventType(ProvisionResultMessage.EventType.ERROR);
            msg.setStatus("FAILED");
            msg.setStep(step);
            msg.setMessage(line);
            msg.setTimestamp(OffsetDateTime.now());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, msg, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                return m;
            });

            log.error("📤 Sent Ansible ERROR event: jobId={}, error={}", jobId,
                    line.length() > 100 ? line.substring(0, 100) + "..." : line);

        } catch (Exception e) {
            log.warn("[AnsibleExecutor] Failed to send ERROR event for jobId={}: {}",
                    jobId, e.getMessage());
        }
    }
}