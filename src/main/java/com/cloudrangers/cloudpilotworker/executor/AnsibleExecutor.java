package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
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

    // ⭐️ Ansible 서버(Worker Node)의 실제 경로 설정
    private static final String ANSIBLE_PLAYBOOK_PATH = "/etc/ansible/main_provision.yml";
    private static final String SSH_KEY_PATH = "/home/admin/.ssh/ansible_key";
    private static final String REMOTE_USER = "admin";

    public AnsibleExecutor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void execute(String targetIp, ProvisionJobMessage msg) {
        log.info("🚀 [Ansible] Starting Provisioning for IP: {}", targetIp);

        try {
            // 1. 설치할 패키지 목록 추출
            List<String> packages = new ArrayList<>();
            if (msg.getProperties() != null && msg.getProperties().getPackages() != null) {
                packages = msg.getProperties().getPackages();
            }

            // 2. Extra Vars 생성 ('{"target_packages": ["nginx", "vscode"]}')
            Map<String, Object> extraVars = new HashMap<>();
            extraVars.put("target_packages", packages);
            String extraVarsJson = objectMapper.writeValueAsString(extraVars);

            // 3. 명령어 조립
            // 명령어 예시: ansible-playbook -i "172.16.5.123," --private-key ... -u admin -e '...' /etc/ansible/main_provision.yml
            List<String> command = new ArrayList<>();
            command.add("ansible-playbook");
            command.add("-i");
            command.add(targetIp + ",");   // ⭐️ 콤마 필수 (Inventory File 없이 실행)
            command.add("--private-key");
            command.add(SSH_KEY_PATH);
            command.add("-u");
            command.add(REMOTE_USER);
            command.add("-e");
            command.add(extraVarsJson);    // JSON 변수 주입
            command.add(ANSIBLE_PLAYBOOK_PATH);

            log.info("[Ansible] Command: {}", String.join(" ", command));

            // 4. 프로세스 실행
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true); // 에러 출력을 표준 출력으로 합침

            // 환경변수 설정 (호스트 키 검사 무시 등 필요시 추가)
            pb.environment().put("ANSIBLE_HOST_KEY_CHECKING", "False");

            Process process = pb.start();

            // 5. 로그 실시간 출력 (Worker 로그 파일에 기록됨)
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    log.info("[Ansible-Log] {}", line);
                }
            }

            // 6. 종료 대기 (최대 20분)
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
            // Ansible 실패가 전체 프로세스를 중단시켜야 한다면 throw e;
            // 여기서는 throw를 해서 WorkerListener에서 로깅 후 처리하도록 함
            throw new RuntimeException("Ansible Execution Failed", e);
        }
    }
}