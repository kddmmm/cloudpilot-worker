package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.log.AnsibleLogContext;
import com.cloudrangers.cloudpilotworker.log.AnsibleLogRefiner;
import com.cloudrangers.cloudpilotworker.log.LogStorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
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

    // ⭐️ Ansible 서버(Worker Node)의 실제 경로 설정
    private static final String ANSIBLE_PLAYBOOK_PATH = "/etc/ansible/main_provision.yml";
    private static final String SSH_KEY_PATH = "/home/admin/.ssh/ansible_key";
    private static final String REMOTE_USER = "admin";

    public AnsibleExecutor(ObjectMapper objectMapper,
                           AnsibleLogRefiner logRefiner,
                           LogStorageService logStorageService) {
        this.objectMapper = objectMapper;
        this.logRefiner = logRefiner;
        this.logStorageService = logStorageService;
    }

    public void execute(String targetIp, ProvisionJobMessage msg, boolean isFinalAttempt) {  // ⭐️ 파라미터 추가
        log.info("🚀 [Ansible] Starting Provisioning for IP: {}", targetIp);

        // 로그 정제를 위한 버퍼 및 컨텍스트 초기화
        StringBuilder refinedLog = new StringBuilder();
        StringBuilder rawLog = new StringBuilder();
        AnsibleLogContext context = new AnsibleLogContext();

        // JobId 추출 (로그 저장용)
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

            // 5. 로그 실시간 출력 및 정제
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    rawLog.append(line).append('\n');

                    String refinedLine = logRefiner.refineLine(line, context);
                    if (refinedLine != null) {
                        refinedLog.append(refinedLine).append('\n');
                        log.info("[Ansible-refined] {}", refinedLine);
                    }

                    log.info("[Ansible-Log] {}", line);
                }
            }

            // 6. 종료 대기
            boolean finished = process.waitFor(20, TimeUnit.MINUTES);
            if (!finished) {
                process.destroyForcibly();
                throw new RuntimeException("Ansible execution timed out.");
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new RuntimeException("Ansible execution failed with exit code: " + exitCode);
            }

            log.info("✅ [Ansible] Provisioning Completed Successfully for IP: {}", targetIp);

        } catch (Exception e) {
            log.error("❌ [Ansible] Execution Error", e);
            throw new RuntimeException("Ansible Execution Failed", e);
        } finally {
            // ⭐️ 마지막 시도일 때만 refined 로그 저장
            try {
                logStorageService.saveLogsToLocal(jobId, "ansible-provision",
                        refinedLog.toString(), rawLog.toString(), isFinalAttempt);
            } catch (Exception e) {
                log.error("Failed to save Ansible logs to local filesystem for jobId: {}", jobId, e);
            }
        }
    }
}