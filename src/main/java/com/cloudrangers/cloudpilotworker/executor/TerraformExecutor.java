package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.dto.ProvisionResultMessage;
import com.cloudrangers.cloudpilotworker.log.LogStorageService;
import com.cloudrangers.cloudpilotworker.log.TerraformLogContext;
import com.cloudrangers.cloudpilotworker.log.TerraformLogRefiner;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.stereotype.Service;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class TerraformExecutor {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(30);
    private static final String VARS_FILE_NAME = "terraform.auto.tfvars.json";

    @Value("${terraform.module.embedded:true}")
    private boolean useEmbeddedModule;

    @Value("${terraform.module.resource-path:/terraform/modules/vsphere-vm}")
    private String moduleResourcePath;

    @Value("${terraform.module.source:}")
    private String moduleSourceProp;

    @Value("${terraform.module.localDir:}")
    private String moduleLocalDir;

    @Value("${worker.id}")
    private String workerId;

    @Value("${rabbitmq.exchange.result.name}")
    private String resultExchange;

    @Value("${rabbitmq.routing-key.result}")
    private String resultRoutingKey;

    private final ResourceLoader resourceLoader;
    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper om;
    private final TerraformLogRefiner logRefiner;
    private final LogStorageService logStorageService;

    // 현재 쓰레드에서 실행 중인 jobId (로그 이벤트에 사용)
    private final ThreadLocal<String> currentJobId = new ThreadLocal<>();
    private final ThreadLocal<StringBuilder> refinedLogBuffer = new ThreadLocal<>();
    private final ThreadLocal<StringBuilder> rawLogBuffer = new ThreadLocal<>();
    private final ThreadLocal<TerraformLogContext> logContext = new ThreadLocal<>();

    public TerraformExecutor(ResourceLoader resourceLoader,
                             RabbitTemplate rabbitTemplate,
                             ObjectMapper objectMapper,
                             TerraformLogRefiner logRefiner,
                             LogStorageService logStorageService) {
        this.resourceLoader = resourceLoader;
        this.rabbitTemplate = rabbitTemplate;
        this.om = objectMapper.copy().enable(SerializationFeature.INDENT_OUTPUT);
        this.logRefiner = logRefiner;
        this.logStorageService = logStorageService;
    }

    // ============================================================
    // 1. 외부 진입점 (Execute)
    // ============================================================

    /**
     * 전체 Terraform 시퀀스 실행:
     *  - init/validate/fmt/plan/apply or destroy
     *  - stdout/stderr → LOG 이벤트
     *  - 최종 결과 → SUCCESS/ERROR 이벤트 (tfRunId/stateUri/instances 포함)
     */
    public String execute(ProvisionJobMessage msg, boolean isFinalAttempt) {
        Objects.requireNonNull(msg, "ProvisionJobMessage must not be null");

        Object rawJobId = msg.getJobId();
        String jobIdStr = rawJobId != null ? String.valueOf(rawJobId) : String.valueOf(System.currentTimeMillis());
        long jobId = parseJobId(rawJobId);

        currentJobId.set(jobIdStr);

        // 로그 정제를 위한 버퍼 및 컨텍스트 초기화
        refinedLogBuffer.set(new StringBuilder());
        rawLogBuffer.set(new StringBuilder());
        TerraformLogContext context = new TerraformLogContext();
        context.setJobId(jobIdStr);
        logContext.set(context);

        // additionalConfig에서 tfRunId, stateUri 추출
        Map<String, Object> additionalConfig = msg.getAdditionalConfig();
        Long tfRunId = extractTfRunId(additionalConfig);
        String stateUriFromBackend = extractStateUri(additionalConfig);

        try {
            String vmName = nvl(msg.getVmName(), "vm-" + jobId);
            String action = resolveAction(msg);

            log.info("[TerraformExecutor] action={}, jobId={}, vmName={}, stateUriFromBackend={}",
                    action, jobIdStr, vmName, stateUriFromBackend);

            // 작업 디렉토리: 항상 /tmp/terraform/{jobId}
            File workDir = new File("/tmp/terraform/" + jobIdStr);
            if (!workDir.exists() && !workDir.mkdirs()) {
                throw new RuntimeException("Failed to create workDir: " + workDir.getAbsolutePath());
            }

            // destroy인 경우 기존 state 재사용 시도
            if ("destroy".equalsIgnoreCase(action)
                    && stateUriFromBackend != null
                    && !stateUriFromBackend.isBlank()) {
                prepareStateForDestroy(workDir, stateUriFromBackend);
            }

            Map<String, Object> tfVars = buildTfVarsFromMessage(msg, vmName);
            Map<String, Object> tfOutputs = Collections.emptyMap();

            try {
                // 실제 Terraform 실행
                execute(jobId, vmName, action, workDir, tfVars);

                if ("apply".equalsIgnoreCase(action)) {
                    // output -json 읽기
                    Map<String, String> env = new HashMap<>();
                    env.put("TF_IN_AUTOMATION", "1");
                    tfOutputs = readTerraformOutputs(workDir, env);

                    // SUCCESS 이벤트 전송 (instances 포함)
                    sendSuccessEvent(jobIdStr, msg, vmName, tfVars, tfOutputs, tfRunId, workDir);
                } else if ("destroy".equalsIgnoreCase(action)) {
                    // Destroy 성공 이벤트
                    sendDestroySuccessEvent(jobIdStr, msg, tfRunId);
                }

                return vmName;
            } catch (RuntimeException e) {
                // 실패 시 ERROR 이벤트 전송
                sendErrorEvent(jobIdStr, msg, e, tfRunId);
                throw e;
            }
        } finally {
            // 로그 저장
            try {
                String refinedLog = refinedLogBuffer.get().toString();
                String rawLog = rawLogBuffer.get().toString();
                logStorageService.saveLogsToLocal(jobIdStr, "terraform", refinedLog, rawLog, isFinalAttempt);
            } catch (Exception e) {
                log.error("Failed to save logs for jobId: {}", jobIdStr, e);
            }

            currentJobId.remove();
            refinedLogBuffer.remove();
            rawLogBuffer.remove();
            logContext.remove();
        }
    }

    /**
     * 실제 Terraform 시퀀스 실행 (init/validate/fmt/plan/apply or destroy)
     */
    public void execute(long jobId, String vmName, String action, File workDir, Map<String, Object> tfVars) {
        Objects.requireNonNull(workDir, "workDir must not be null");
        if (!workDir.exists() && !workDir.mkdirs()) {
            throw new RuntimeException("Failed to create workDir: " + workDir);
        }

        log.info("Starting Terraform execution for jobId={}, vmName={}, action={}", jobId, vmName, action);

        Map<String, String> env = new HashMap<>();
        env.put("TF_IN_AUTOMATION", "1");

        // 모듈 준비 (z_automation_outputs.tf 자동 생성 포함)
        ensureModulePresent(workDir, env);

        // destroy 시에도 빈 tfvars라도 파일은 만들어 둠
        File varsFile = new File(workDir, VARS_FILE_NAME);
        if (tfVars != null && !tfVars.isEmpty()) {
            writeVarsFile(varsFile, tfVars);
            log.info("Wrote vars file: {} (keys: {})", varsFile.getAbsolutePath(), tfVars.keySet());
        } else {
            log.warn("tfVars is null or empty - creating empty vars file");
            writeVarsFile(varsFile, Collections.emptyMap());
        }

        runTerraformSequence(workDir, env, action);
    }

    public void execute(File workDir, String action) {
        Map<String, String> env = Map.of("TF_IN_AUTOMATION", "1");
        ensureModulePresent(workDir, env);
        runTerraformSequence(workDir, env, action);
    }

    // ============================================================
    // [IP 추출] (WorkerListener에서 호출 가능)
    // ============================================================

    public String getProvisionedIp(String jobId) {
        File workDir = new File("/tmp/terraform/" + jobId);
        if (!workDir.exists()) {
            log.warn("[Terraform] Working directory not found for jobId: {}", jobId);
            return null;
        }

        try {
            ProcessBuilder pb = new ProcessBuilder("terraform", "output", "-json");
            pb.directory(workDir);
            Process p = pb.start();

            String jsonOutput = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            p.waitFor();

            if (p.exitValue() != 0) {
                log.warn("[Terraform] Failed to get output json. Exit code: {}", p.exitValue());
                return null;
            }

            Map<String, Map<String, Object>> outputs = om.readValue(jsonOutput, Map.class);

            // 1. worker_guest_ips 에서 172.16.* 우선 검색
            if (outputs.containsKey("worker_guest_ips")) {
                Object val = outputs.get("worker_guest_ips").get("value");
                if (val instanceof List<?> ips) {
                    for (Object obj : ips) {
                        String ip = String.valueOf(obj);
                        if (ip.startsWith("172.16.")) {
                            log.info("[Terraform] Found Internal IP from worker_guest_ips: {}", ip);
                            return ip;
                        }
                    }
                }
            }

            // 2. worker_ip_address
            if (outputs.containsKey("worker_ip_address")) {
                String ip = String.valueOf(outputs.get("worker_ip_address").get("value"));
                if (ip.startsWith("172.16.")) return ip;
            }

            // 3. vm_ip_addresses
            if (outputs.containsKey("vm_ip_addresses")) {
                Object val = outputs.get("vm_ip_addresses").get("value");
                if (val instanceof List<?> ips) {
                    for (Object obj : ips) {
                        String ip = String.valueOf(obj);
                        if (ip.startsWith("172.16.")) {
                            log.info("[Terraform] Found Internal IP from vm_ip_addresses: {}", ip);
                            return ip;
                        }
                    }
                }
            }

            // 4. 최후의 수단: 아무 IP나
            if (outputs.containsKey("worker_ip_address")) {
                return String.valueOf(outputs.get("worker_ip_address").get("value"));
            }
            if (outputs.containsKey("ip_address")) {
                return String.valueOf(outputs.get("ip_address").get("value"));
            }

            log.warn("[Terraform] No valid IP found. Available Keys: {}", outputs.keySet());
            return null;

        } catch (Exception e) {
            log.error("[Terraform] Failed to parse IP address", e);
            return null;
        }
    }

    // ============================================================
    // 모듈 스테이징 & 자동 Output 파일 생성
    // ============================================================

    private void ensureModulePresent(File dir, Map<String, String> env) {
        log.debug("=== ensureModulePresent START ===");
        log.debug("Working directory: {}", dir.getAbsolutePath());
        log.debug("useEmbeddedModule: {}", useEmbeddedModule);
        log.debug("moduleResourcePath: '{}'", moduleResourcePath);

        // 이미 .tf 파일이 있으면 모듈 있다고 보고 output 파일만 보장
        if (hasTfFiles(dir)) {
            log.info("Terraform config already present in {}", dir.getAbsolutePath());
            generateExtraOutputsFile(dir);
            return;
        }

        log.info("No .tf files found. Starting module staging...");

        // 1) 클래스패스 내 embedded module
        if (useEmbeddedModule && isNotBlank(moduleResourcePath)) {
            log.info("Using embedded module from classpath: {}", moduleResourcePath);
            try {
                stageModuleFromClasspath(dir);
                generateExtraOutputsFile(dir);
                return;
            } catch (Exception e) {
                log.error("Failed to stage embedded module", e);
                throw new RuntimeException("Failed to stage embedded module from classpath: " + moduleResourcePath, e);
            }
        }

        // 2) terraform.module.source / TERRAFORM_MODULE_SOURCE
        String src = normalizeEmptyToNull(moduleSourceProp);
        if (src == null) src = normalizeEmptyToNull(System.getenv("TERRAFORM_MODULE_SOURCE"));
        log.debug("Resolved module source: '{}'", src);

        if (src != null) {
            log.info("Using module source: {}", src);
            stageModuleFromSource(dir, env, src);
            generateExtraOutputsFile(dir);
            return;
        }

        // 3) 로컬 디렉토리
        String localDir = normalizeEmptyToNull(moduleLocalDir);
        log.debug("Checking local module directory: '{}'", localDir);

        if (localDir != null) {
            Path localPath = Path.of(localDir);
            if (Files.exists(localPath) && Files.isDirectory(localPath)) {
                log.info("Using local module directory: {}", localDir);
                stageModuleFromLocal(dir, env, localPath);
                generateExtraOutputsFile(dir);
                return;
            } else {
                log.warn("Local module directory not found or not a directory: {}", localDir);
            }
        }

        log.error("No Terraform module source configured!");
        throw new RuntimeException("No Terraform configuration found and no module source configured.");
    }

    // z_automation_outputs.tf 자동 생성
    private void generateExtraOutputsFile(File dir) {
        File outputFile = new File(dir, "z_automation_outputs.tf");
        try (FileWriter writer = new FileWriter(outputFile, false)) {
            writer.write("""
                # === Auto-generated by Worker for Automation ===
                #
                # destroy 모드에서 vsphere_virtual_machine.vm 이 0개가 될 수 있으므로
                # try(...) 로 감싸서 validation / apply 단계에서 터지지 않도록 방어한다.
                
                output "worker_ip_address" {
                  value       = try(vsphere_virtual_machine.vm[0].default_ip_address, null)
                  description = "Worker internal use: Default IP"
                }
                
                # All guest IP addresses
                output "worker_guest_ips" {
                  value       = try(vsphere_virtual_machine.vm[0].guest_ip_addresses, [])
                  description = "Worker internal use: All Guest IPs"
                }
                
                output "worker_vm_ids" {
                  value       = try(vsphere_virtual_machine.vm[*].id, [])
                  description = "Worker internal use: VM IDs"
                }
                """);
            log.info("Created z_automation_outputs.tf for IP extraction.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate extra outputs file", e);
        }
    }

    private void stageModuleFromClasspath(File targetDir) throws IOException {
        log.info("Extracting embedded Terraform module to {}", targetDir.getAbsolutePath());

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String pattern = "classpath:" + moduleResourcePath + "/**/*.tf";

        Resource[] resources = resolver.getResources(pattern);
        if (resources == null || resources.length == 0) {
            pattern = "classpath:" + moduleResourcePath + "/**/*.tf.json";
            resources = resolver.getResources(pattern);
        }

        if (resources == null || resources.length == 0) {
            throw new IOException("No Terraform files found in classpath: " + moduleResourcePath);
        }

        log.info("Found {} Terraform file(s) in classpath", resources.length);

        for (Resource resource : resources) {
            String filename = resource.getFilename();
            if (filename == null) continue;

            File targetFile = new File(targetDir, filename);
            try (InputStream is = resource.getInputStream();
                 FileOutputStream fos = new FileOutputStream(targetFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
            }
        }

        log.info("Extracted {} Terraform files", resources.length);

        if (!hasTfFiles(targetDir)) {
            throw new IOException("Extraction completed but no .tf files found in " + targetDir);
        }
    }

    private void stageModuleFromSource(File dir, Map<String, String> env, String source) {
        log.info("Staging module via 'terraform init -from-module={}'", source);

        try {
            executeCommand(dir, env, "terraform", "init", "-from-module=" + source, "-input=false", "-no-color");
            log.info("Module staged from source");

            executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
            log.info("Providers initialized");

            if (!hasTfFiles(dir)) {
                throw new RuntimeException("Module staging completed but no .tf files found");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to stage module from source: " + source, e);
        }
    }

    private void stageModuleFromLocal(File dir, Map<String, String> env, Path localPath) {
        log.info("Staging module by copying from: {}", localPath);

        try {
            copyDirectoryRecursively(localPath, dir.toPath());
            log.info("Module files copied");

            if (!hasTfFiles(dir)) {
                throw new RuntimeException("Copied from local directory but no .tf files present");
            }

            executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
            log.info("Providers initialized");

        } catch (Exception e) {
            throw new RuntimeException("Failed to stage module from local directory: " + localPath, e);
        }
    }

    private boolean hasTfFiles(File dir) {
        File[] list = dir.listFiles((d, name) -> name.endsWith(".tf") || name.endsWith(".tf.json"));
        return list != null && list.length > 0;
    }

    private void copyDirectoryRecursively(Path src, Path dst) {
        try {
            if (!Files.exists(dst)) Files.createDirectories(dst);
            Files.walk(src).forEach(p -> {
                try {
                    Path rel = src.relativize(p);
                    Path target = dst.resolve(rel);
                    if (Files.isDirectory(p)) {
                        if (!Files.exists(target)) Files.createDirectories(target);
                    } else {
                        Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to copy module directory: " + src + " -> " + dst, e);
        }
    }

    private String normalizeEmptyToNull(String s) {
        if (s == null || s.trim().isEmpty()) return null;
        return s.trim();
    }

    // ============================================================
    // Terraform 실행 시퀀스 + 커맨드
    // ============================================================

    /**
     * destroy 시 기존 state 파일을 작업 디렉토리로 복사
     */
    private void prepareStateForDestroy(File workDir, String stateUri) {
        if (stateUri == null || stateUri.isBlank()) {
            log.info("[TerraformExecutor] No stateUri provided; destroy will run with empty state.");
            return;
        }

        try {
            Path srcPath = Paths.get(stateUri);

            // stateUri가 디렉터리인 경우 내부의 terraform.tfstate 사용
            if (Files.isDirectory(srcPath)) {
                Path tfState = srcPath.resolve("terraform.tfstate");
                if (Files.exists(tfState)) {
                    srcPath = tfState;
                } else {
                    log.warn("[TerraformExecutor] stateUri is directory but terraform.tfstate not found: {}", tfState);
                    return;
                }
            }

            if (!Files.exists(srcPath)) {
                log.warn("[TerraformExecutor] state file does not exist: {}", srcPath);
                return;
            }

            Files.createDirectories(workDir.toPath());
            Path destPath = workDir.toPath().resolve("terraform.tfstate");

            Files.copy(srcPath, destPath, StandardCopyOption.REPLACE_EXISTING);

            log.info("[TerraformExecutor] Prepared state file for destroy: {} -> {}",
                    srcPath, destPath);
        } catch (Exception e) {
            log.error("[TerraformExecutor] Failed to prepare state for destroy. stateUri={}", stateUri, e);
        }
    }

    private void runTerraformSequence(File dir, Map<String, String> env, String action) {
        executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
        executeCommand(dir, env, "terraform", "validate", "-no-color");
        executeCommandOrSkip(dir, env, false, "terraform", "fmt", "-recursive", "-write=true");

        if ("apply".equalsIgnoreCase(action)) {
            executeCommand(dir, env, "terraform", "plan", "-input=false", "-no-color", "-out=plan.tfplan");
            executeCommand(dir, env, "terraform", "apply", "-input=false", "-no-color", "-auto-approve", "plan.tfplan");
        } else if ("destroy".equalsIgnoreCase(action)) {
            executeCommand(dir, env, "terraform", "plan", "-destroy", "-input=false", "-no-color", "-out=destroy.tfplan");
            executeCommand(dir, env, "terraform", "apply", "-input=false", "-no-color", "-auto-approve", "destroy.tfplan");
        } else {
            executeCommand(dir, env, "terraform", "plan", "-input=false", "-no-color");
        }
    }

    private void executeCommandOrSkip(File dir, Map<String, String> env, boolean failHard, String... cmd) {
        try {
            executeCommand(dir, env, cmd);
        } catch (RuntimeException e) {
            if (!failHard) {
                log.warn("[Terraform][non-fatal] '{}' 실패(무시하고 진행): {}",
                        String.join(" ", cmd), firstLine(e.getMessage()));
                return;
            }
            throw wrapTerraformException(e);
        }
    }

    private void executeCommand(File dir, Map<String, String> env, String... cmd) {
        String display = String.join(" ", cmd);
        log.info("Executing: {} (dir: {})", display, dir.getAbsolutePath());

        final String jobId = currentJobId.get();
        final String step = resolveStepName(cmd);

        ProcessBuilder pb = new ProcessBuilder(cmd)
                .directory(dir)
                .redirectErrorStream(false);

        if (env != null && !env.isEmpty()) {
            pb.environment().putAll(env);
        }

        try {
            Process p = pb.start();

            StringBuilder stdoutBuf = new StringBuilder();
            StringBuilder stderrBuf = new StringBuilder();
            CountDownLatch latch = new CountDownLatch(2);

            TerraformLogContext ctx = logContext.get();
            StringBuilder refinedBuffer = refinedLogBuffer.get();
            StringBuilder rawBuffer = rawLogBuffer.get();

            Thread tOut = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        stdoutBuf.append(line).append('\n');

                        // 1) 원본 로그는 파일용 버퍼에만 저장
                        if (rawBuffer != null) {
                            rawBuffer.append(line).append('\n');
                        }

                        // 2) 정제
                        String refinedLine = null;
                        if (ctx != null) {
                            refinedLine = logRefiner.refineLine(line, ctx);
                        }

                        // 3) 콘솔 로그는 원본 그대로 (원하면 여기도 줄일 수 있음)
                        log.info("[terraform] {}", line);

                        // 4) 정제된 라인만 큐로 전송 (에러 상태면 ERROR 이벤트로)
                        if (refinedLine != null) {
                            if (refinedBuffer != null) {
                                refinedBuffer.append(refinedLine).append('\n');
                            }

                            if (ctx != null && ctx.isInError()) {
                                log.error("[terraform-refined-error] {}", refinedLine);
                                sendErrorLogEvent(jobId, step, refinedLine);
                            } else {
                                log.info("[terraform-refined] {}", refinedLine);
                                sendLogEvent(jobId, step, refinedLine);
                            }
                        }
                    }
                } catch (IOException ignore) {
                } finally {
                    latch.countDown();
                }
            }, "tf-stdout");


            Thread tErr = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(p.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        stderrBuf.append(line).append('\n');

                        // 1) 원본 로그는 파일용 버퍼에만 저장
                        if (rawBuffer != null) {
                            rawBuffer.append("[stderr] ").append(line).append('\n');
                        }

                        // 2) 정제
                        String refinedLine = null;
                        if (ctx != null) {
                            refinedLine = logRefiner.refineLine(line, ctx);
                        }

                        // 3) 콘솔 로그는 원본 그대로
                        log.error("[terraform] {}", line);

                        // 4) 정제된 라인만 큐로 전송 (stderr는 항상 ERROR 이벤트로)
                        if (refinedLine != null) {
                            if (refinedBuffer != null) {
                                refinedBuffer.append(refinedLine).append('\n');
                            }
                            log.error("[terraform-refined-error] {}", refinedLine);
                            sendErrorLogEvent(jobId, step, refinedLine);
                        }
                    }
                } catch (IOException ignore) {
                } finally {
                    latch.countDown();
                }
            }, "tf-stderr");


            tOut.start();
            tErr.start();

            boolean finished = p.waitFor(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                p.destroyForcibly();
                String msg = "Command timed out: " + display;
                sendErrorLogEvent(jobId, step, msg);
                throw new RuntimeException(msg);
            }

            latch.await(5, TimeUnit.SECONDS);

            int exit = p.exitValue();
            if (exit != 0) {
                String merged = mergeOutErr(stdoutBuf.toString(), stderrBuf.toString());
                String msg = "Command failed: " + display + " (exit " + exit + ")\n" + merged;
                sendErrorLogEvent(jobId, step, firstLine(msg));
                throw wrapTerraformException(new RuntimeException(msg));
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            String msg = "Command interrupted: " + display + " - " + ie.getMessage();
            sendErrorLogEvent(jobId, step, firstLine(msg));
            throw wrapTerraformException(new RuntimeException(msg, ie));
        } catch (IOException e) {
            String msg = "Command failed to start/run: " + display + " - " + e.getMessage();
            sendErrorLogEvent(jobId, step, firstLine(msg));
            throw wrapTerraformException(new RuntimeException(msg, e));
        }
    }

    private String resolveStepName(String... cmd) {
        if (cmd == null || cmd.length == 0) return "terraform";
        if (cmd.length >= 2 && "terraform".equals(cmd[0])) {
            return "terraform_" + cmd[1];
        }
        return String.join("_", cmd);
    }

    // ============================================================
    // LOG / SUCCESS / ERROR 이벤트 전송
    // ============================================================

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
        } catch (Exception e) {
            log.warn("[TerraformExecutor] Failed to send LOG event for jobId={}: {}", jobId, e.getMessage());
        }
    }

    /**
     * 실패 시 ERROR 이벤트 전송 (LOG 대신 ERROR 타입 사용)
     */
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
        } catch (Exception e) {
            log.warn("[TerraformExecutor] Failed to send ERROR event for jobId={}: {}", jobId, e.getMessage());
        }
    }

    /**
     * Terraform apply 성공 후 SUCCESS 이벤트 전송.
     * tfRunId / stateUri / instances 포함.
     */
    private void sendSuccessEvent(String jobId,
                                  ProvisionJobMessage msg,
                                  String vmName,
                                  Map<String, Object> tfVars,
                                  Map<String, Object> tfOutputs,
                                  Long tfRunId,
                                  File workDir) {
        if (jobId == null) return;
        if (tfOutputs == null) tfOutputs = Collections.emptyMap();

        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.SUCCESS);
            result.setStatus("SUCCEEDED");
            result.setStep("terraform_apply");
            result.setTimestamp(OffsetDateTime.now());
            result.setTfRunId(tfRunId);

            // stateUri (로컬 경로 기준)
            if (workDir != null) {
                File stateFile = new File(workDir, "terraform.tfstate");
                if (stateFile.exists()) {
                    result.setStateUri(stateFile.getAbsolutePath());
                    log.info("State URI set: {}", stateFile.getAbsolutePath());
                }
            }

            // vmCount 계산
            int vmCount = 1;
            Object fromVars = tfVars != null ? tfVars.get("vm_count") : null;
            if (fromVars instanceof Number n) {
                vmCount = Math.max(1, n.intValue());
            } else if (msg.getVmCount() != null) {
                vmCount = Math.max(1, msg.getVmCount());
            }

            // InstanceInfo 리스트 생성
            List<ProvisionResultMessage.InstanceInfo> instances =
                    buildInstanceInfos(msg, vmName, vmCount, tfVars, tfOutputs);
            result.setInstances(instances);

            // vmId (가능하면 첫 번째 인스턴스의 externalId, 없으면 name)
            if (!instances.isEmpty()) {
                ProvisionResultMessage.InstanceInfo first = instances.get(0);
                if (isNotBlank(first.getExternalId())) {
                    result.setVmId(first.getExternalId());
                } else if (isNotBlank(first.getName())) {
                    result.setVmId(first.getName());
                }
            }

            // 메시지
            result.setMessage("Terraform apply succeeded (" + vmCount + " VM(s))");

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                if (tfRunId != null) {
                    m.getMessageProperties().setHeader("tfRunId", tfRunId);
                }
                return m;
            });

            log.info("[TerraformExecutor] Sent SUCCESS event for jobId={}, tfRunId={}, stateUri={}, vmCount={}",
                    jobId, tfRunId, result.getStateUri(), vmCount);
        } catch (Exception e) {
            log.warn("[TerraformExecutor] Failed to send SUCCESS event for jobId={}: {}", jobId, e.getMessage());
        }
    }

    /**
     * Destroy 성공 이벤트 전송
     */
    private void sendDestroySuccessEvent(String jobId, ProvisionJobMessage msg, Long tfRunId) {
        if (jobId == null) return;

        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.SUCCESS);
            result.setStatus("SUCCEEDED");
            result.setStep("terraform_destroy");
            result.setTimestamp(OffsetDateTime.now());
            result.setTfRunId(tfRunId);
            result.setMessage("Terraform destroy succeeded");

            // 원본 요청 메시지에서 vmId 전달 (BE에서 vm_instance 삭제 처리에 사용)
            String vmIdStr = extractVmIdFromMsg(msg);
            if (isNotBlank(vmIdStr)) {
                result.setVmId(vmIdStr);
            }

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                if (tfRunId != null) {
                    m.getMessageProperties().setHeader("tfRunId", tfRunId);
                }
                return m;
            });

            log.info("[TerraformExecutor] Sent DESTROY SUCCESS event for jobId={}, tfRunId={}, vmId={}",
                    jobId, tfRunId, vmIdStr);
        } catch (Exception e) {
            log.warn("[TerraformExecutor] Failed to send DESTROY SUCCESS event: {}", e.getMessage());
        }
    }

    /**
     * Terraform 실행 과정에서 예외 발생 시 ERROR 이벤트 전송
     */
    private void sendErrorEvent(String jobId,
                                ProvisionJobMessage srcMsg,
                                RuntimeException e,
                                Long tfRunId) {
        if (jobId == null) return;

        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.ERROR);
            result.setStatus("FAILED");
            result.setStep("terraform");
            result.setTimestamp(OffsetDateTime.now());
            result.setMessage(firstLine(e.getMessage()));
            result.setTfRunId(tfRunId);

            // destroy 실패 시에도 vmId를 같이 보내서 BE에서 lifecycle 롤백 가능하게
            String vmIdStr = extractVmIdFromMsg(srcMsg);
            if (isNotBlank(vmIdStr)) {
                result.setVmId(vmIdStr);
            }

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                if (tfRunId != null) {
                    m.getMessageProperties().setHeader("tfRunId", tfRunId);
                }
                return m;
            });

            log.warn("[TerraformExecutor] Sent ERROR event for jobId={}, tfRunId={}, vmId={}: {}",
                    jobId, tfRunId, vmIdStr, firstLine(e.getMessage()));
        } catch (Exception ex) {
            log.warn("[TerraformExecutor] Failed to send ERROR event for jobId={}: {}", jobId, ex.getMessage());
        }
    }

    // ============================================================
    // terraform output -json 파싱
    // ============================================================

    private Map<String, Object> readTerraformOutputs(File dir, Map<String, String> env) {
        String display = "terraform output -json";
        log.info("Executing: {} (dir: {})", display, dir.getAbsolutePath());

        final String jobId = currentJobId.get();
        final String step = "terraform_output";

        ProcessBuilder pb = new ProcessBuilder("terraform", "output", "-json")
                .directory(dir)
                .redirectErrorStream(false);

        if (env != null && !env.isEmpty()) {
            pb.environment().putAll(env);
        }

        try {
            Process p = pb.start();

            StringBuilder stdoutBuf = new StringBuilder();
            StringBuilder stderrBuf = new StringBuilder();
            CountDownLatch latch = new CountDownLatch(2);

            Thread tOut = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stdoutBuf.append(line).append('\n');
                        log.info("[terraform-output] {}", line);
                    }
                } catch (IOException ignore) {
                } finally {
                    latch.countDown();
                }
            }, "tf-output-stdout");

            Thread tErr = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(p.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stderrBuf.append(line).append('\n');
                        log.warn("[terraform-output] {}", line);
                    }
                } catch (IOException ignore) {
                } finally {
                    latch.countDown();
                }
            }, "tf-output-stderr");

            tOut.start();
            tErr.start();

            boolean finished = p.waitFor(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) {
                p.destroyForcibly();
                String msg = "Command timed out: " + display;
                sendLogEvent(jobId, step, msg);
                log.warn(msg);
                return Collections.emptyMap();
            }

            latch.await(5, TimeUnit.SECONDS);

            int exit = p.exitValue();
            if (exit != 0) {
                String merged = mergeOutErr(stdoutBuf.toString(), stderrBuf.toString());
                String msg = "Command failed: " + display + " (exit " + exit + ")\n" + merged;
                sendLogEvent(jobId, step, firstLine(msg));
                log.warn(msg);
                return Collections.emptyMap();
            }

            String json = stdoutBuf.toString();
            if (json.isBlank()) {
                log.warn("[TerraformExecutor] terraform output -json produced empty stdout");
                return Collections.emptyMap();
            }

            Map<String, TerraformOutput> raw = om.readValue(
                    json,
                    new TypeReference<Map<String, TerraformOutput>>() {}
            );

            Map<String, Object> flat = new LinkedHashMap<>();
            for (Map.Entry<String, TerraformOutput> e : raw.entrySet()) {
                TerraformOutput v = e.getValue();
                flat.put(e.getKey(), v != null ? v.value : null);
            }

            log.info("[TerraformExecutor] Parsed terraform outputs keys = {}", flat.keySet());
            return flat;
        } catch (Exception e) {
            String msg = "Failed to read terraform outputs: " + e.getMessage();
            sendLogEvent(jobId, step, firstLine(msg));
            log.warn(msg, e);
            return Collections.emptyMap();
        }
    }

    private List<Map<String, Object>> extractVmDetails(Map<String, Object> tfOutputs) {
        if (tfOutputs == null) return Collections.emptyList();
        Object detailsObj = tfOutputs.get("vm_details");
        if (!(detailsObj instanceof List<?> list)) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> result = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof Map<?, ?> m) {
                Map<String, Object> casted = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    casted.put(String.valueOf(e.getKey()), e.getValue());
                }
                result.add(casted);
            }
        }
        return result;
    }

    // terraform output 구조용 DTO
    private static class TerraformOutput {
        public Object value;
    }

    // ============================================================
    // 메시지 → tfvars 매핑
    // ============================================================

    private Map<String, Object> buildTfVarsFromMessage(ProvisionJobMessage msg, String vmName) {
        Map<String, Object> tf = new LinkedHashMap<>();

        String action = resolveAction(msg);

        // operation 설정
        String operation = "destroy".equalsIgnoreCase(action) ? "destroy" : "create";
        tf.put("operation", operation);

        // destroy 시 최소 변수만
        if ("destroy".equalsIgnoreCase(action)) {
            log.info("[TerraformExecutor] Destroy action detected - using minimal tfvars");

            tf.put("vsphere_server", nvl(System.getenv("VSPHERE_SERVER"), "vcenter.fisa.com"));
            tf.put("vsphere_user", nvl(System.getenv("VSPHERE_USER"), "administrator@fisa.ce5"));
            tf.put("vsphere_password", System.getenv("VSPHERE_PASSWORD"));
            tf.put("allow_unverified_ssl", Boolean.parseBoolean(nvl(System.getenv("VSPHERE_ALLOW_UNVERIFIED_SSL"), "true")));

            Object datacenterObj = msg.getAdditionalConfig() != null
                    ? msg.getAdditionalConfig().getOrDefault("datacenter", null)
                    : null;

            tf.put("datacenter", nvl(
                    datacenterObj != null ? String.valueOf(datacenterObj) : null,
                    nvl(System.getenv("VSPHERE_DATACENTER"), "ce5-3")
            ));

            Object clusterObj = msg.getAdditionalConfig() != null
                    ? msg.getAdditionalConfig().getOrDefault("cluster", null)
                    : null;
            tf.put("cluster", nvl(
                    clusterObj != null ? String.valueOf(clusterObj) : null,
                    nvl(System.getenv("VSPHERE_CLUSTER"), "")
            ));

            Object datastoreObj = msg.getAdditionalConfig() != null
                    ? msg.getAdditionalConfig().getOrDefault("datastore", null)
                    : null;
            tf.put("datastore", nvl(
                    datastoreObj != null ? String.valueOf(datastoreObj) : null,
                    nvl(System.getenv("VSPHERE_DATASTORE"), "HDD1 (1)")
            ));

            // 더미 값 (count=1, destroy 모드에서 실제 사용 X)
            tf.put("vm_name", vmName);
            tf.put("vm_count", 1);
            tf.put("template_name", "dummy-not-used");
            tf.put("network", "PG-WAN");
            tf.put("cpu_cores", 2);
            tf.put("memory_gb", 4);
            tf.put("disk_gb", 50);

            log.info("[TerraformExecutor] Destroy tfvars: {}", tf);
            return tf;
        }

        // apply 시 전체 tfvars 구성
        log.info("[TerraformExecutor] Apply action detected - building full tfvars");

        tf.put("vsphere_server", nvl(System.getenv("VSPHERE_SERVER"), "vcenter.fisa.com"));
        tf.put("vsphere_user", nvl(System.getenv("VSPHERE_USER"), "administrator@fisa.ce5"));
        tf.put("vsphere_password", System.getenv("VSPHERE_PASSWORD"));
        tf.put("allow_unverified_ssl", Boolean.parseBoolean(nvl(System.getenv("VSPHERE_ALLOW_UNVERIFIED_SSL"), "true")));

        Object datacenterObj = msg.getAdditionalConfig() != null
                ? msg.getAdditionalConfig().getOrDefault("datacenter", null)
                : null;
        tf.put("datacenter", nvl(
                datacenterObj != null ? String.valueOf(datacenterObj) : null,
                nvl(System.getenv("VSPHERE_DATACENTER"), "ce5-3")
        ));

        Object clusterObj = msg.getAdditionalConfig() != null
                ? msg.getAdditionalConfig().getOrDefault("cluster", null)
                : null;
        tf.put("cluster", nvl(
                clusterObj != null ? String.valueOf(clusterObj) : null,
                nvl(System.getenv("VSPHERE_CLUSTER"), "")
        ));

        Object datastoreObj = msg.getAdditionalConfig() != null
                ? msg.getAdditionalConfig().getOrDefault("datastore", null)
                : null;
        tf.put("datastore", nvl(
                datastoreObj != null ? String.valueOf(datastoreObj) : null,
                nvl(System.getenv("VSPHERE_DATASTORE"), "HDD1 (1)")
        ));

        tf.put("folder", nvl(
                msg.getAdditionalConfig() != null ? (String) msg.getAdditionalConfig().getOrDefault("folder", "") : "",
                ""
        ));

        tf.put("vm_name", vmName);
        tf.put("vm_count", msg.getVmCount() != null ? msg.getVmCount() : 1);

        Map<String, Object> add = msg.getAdditionalConfig() != null
                ? msg.getAdditionalConfig()
                : Collections.emptyMap();

        String templateName = (String) add.get("templateName");
        if (!isNotBlank(templateName) && msg.getTemplate() != null) {
            templateName = msg.getTemplate().getItemName();
        }
        tf.put("template_name", templateName);

        tf.put("cpu_cores", msg.getCpuCores() != null ? msg.getCpuCores() : 2);
        tf.put("memory_gb", msg.getMemoryGb() != null ? msg.getMemoryGb() : 4);
        tf.put("disk_gb", msg.getDiskGb() != null ? msg.getDiskGb() : 50);

        String diskProv = asString(add.getOrDefault("diskProvisioning", "thin"));
        tf.put("disk_provisioning", diskProv.toLowerCase());

        Long teamId = msg.getTeamId();
        Long zoneId = msg.getZoneId();

        List<String> networks = resolveNetworkNames(msg, add, teamId, zoneId);
        if (!networks.isEmpty()) {
            tf.put("network", networks.get(0));
            if (networks.size() > 1) {
                tf.put("extra_networks", networks.subList(1, networks.size()));
            }
        } else {
            String defaultNet = normalizeEmptyToNull(System.getenv("VSPHERE_NETWORK"));
            if (defaultNet == null) defaultNet = "PG-WAN";
            tf.put("network", defaultNet);
        }

        String ipMode = asString(add.getOrDefault("ipAllocationMode", "DHCP"));
        tf.put("ip_allocation_mode", ipMode.toUpperCase());

        ProvisionJobMessage.NetSpec net = msg.getNet();
        if (net != null) {
            ProvisionJobMessage.NetSpec.Ipv4 ipv4 = net.getIpv4();
            if (ipv4 != null) {
                if (isNotBlank(ipv4.getAddress())) {
                    tf.put("ipv4_address", ipv4.getAddress());
                }
                if (ipv4.getPrefix() != null) {
                    tf.put("ipv4_netmask", ipv4.getPrefix());
                }
                if (isNotBlank(ipv4.getGateway())) {
                    tf.put("ipv4_gateway", ipv4.getGateway());
                }
            }

            List<String> dnsList = net.getDns();
            if (dnsList != null && !dnsList.isEmpty()) {
                tf.put("dns_servers", new ArrayList<>(dnsList));
            }
        }

        tf.put("domain", "local");

        Map<String, String> tags = msg.getTags();
        if (tags != null) {
            tf.put("tags", tags);
        } else {
            tf.put("tags", Collections.emptyMap());
        }

        log.info("[TerraformExecutor] Apply tfvars keys: {}", tf.keySet());
        return tf;
    }

    /**
     * 팀/존/요청 additionalConfig를 기반으로 vSphere 네트워크 목록 결정
     */
    private List<String> resolveNetworkNames(ProvisionJobMessage msg,
                                             Map<String, Object> additionalConfig,
                                             Long teamId,
                                             Long zoneId) {
        List<String> networks = new ArrayList<>();

        if (additionalConfig != null) {
            Object networksObj = additionalConfig.get("networks");
            if (networksObj instanceof List<?> list) {
                for (Object o : list) {
                    if (o == null) continue;
                    String s = String.valueOf(o).trim();
                    if (isNotBlank(s)) {
                        networks.add(s);
                    }
                }
            }
        }

        if (networks.isEmpty() && additionalConfig != null) {
            Object networkFromReq = additionalConfig.get("network");
            if (networkFromReq != null) {
                String s = String.valueOf(networkFromReq).trim();
                if (isNotBlank(s)) {
                    networks.add(s);
                }
            }
        }

        if (networks.isEmpty() && msg.getNet() != null && isNotBlank(msg.getNet().getIface())) {
            networks.add(msg.getNet().getIface());
        }

        if (teamId != null) {
            if (teamId == 1L) {
                String teamVlan = "VLAN101_TeamA";
                if (!networks.contains(teamVlan)) {
                    networks.add(0, teamVlan);
                }
                log.info("[TerraformExecutor] teamId={} → network='{}'", teamId, teamVlan);
            }
            // 다른 팀 매핑은 필요 시 추가
        }

        if (networks.isEmpty()) {
            String fromEnv = normalizeEmptyToNull(System.getenv("VSPHERE_NETWORK"));
            if (fromEnv != null) {
                networks.add(fromEnv);
                log.info("[TerraformExecutor] Using default vSphere network from env VSPHERE_NETWORK={}", fromEnv);
            } else {
                networks.add("PG-WAN");
                log.info("[TerraformExecutor] No explicit network → fallback to PG-WAN");
            }
        }

        log.info("[TerraformExecutor] Resolved networks for teamId={}, zoneId={} => {}",
                teamId, zoneId, networks);

        return networks;
    }

    // ============================================================
    // InstanceInfo 빌드
    // ============================================================

    private List<ProvisionResultMessage.InstanceInfo> buildInstanceInfos(ProvisionJobMessage msg,
                                                                         String baseVmName,
                                                                         int vmCount,
                                                                         Map<String, Object> tfVars,
                                                                         Map<String, Object> tfOutputs) {
        List<ProvisionResultMessage.InstanceInfo> result = new ArrayList<>();

        List<Map<String, Object>> vmDetails = extractVmDetails(tfOutputs);
        if (!vmDetails.isEmpty()) {
            for (int i = 0; i < vmDetails.size(); i++) {
                Map<String, Object> detail = vmDetails.get(i);
                ProvisionResultMessage.InstanceInfo info =
                        buildInstanceInfoFromDetail(msg, baseVmName, i, detail, tfVars, tfOutputs);
                result.add(info);
            }
            return result;
        }

        // vm_details 없으면 Fallback
        List<String> vmIds = extractVmIdsFromOutputs(tfOutputs);
        List<String> allIps = extractAllIpsFromOutputs(tfOutputs);

        for (int i = 0; i < vmCount; i++) {
            ProvisionResultMessage.InstanceInfo info =
                    buildInstanceInfoFallback(msg, baseVmName, i, vmIds, allIps, tfVars);
            result.add(info);
        }

        return result;
    }

    private ProvisionResultMessage.InstanceInfo buildInstanceInfoFromDetail(ProvisionJobMessage msg,
                                                                            String baseVmName,
                                                                            int index,
                                                                            Map<String, Object> detail,
                                                                            Map<String, Object> tfVars,
                                                                            Map<String, Object> tfOutputs) {
        ProvisionResultMessage.InstanceInfo info = new ProvisionResultMessage.InstanceInfo();

        // name
        String name = asString(firstNonNull(
                detail.get("name"),
                detail.get("hostname"),
                baseVmNameForIndex(baseVmName, index)
        ));
        info.setName(name);

        // externalId
        String externalId = asString(firstNonNull(
                detail.get("id"),
                detail.get("external_id"),
                detail.get("moid"),
                detail.get("instance_id")
        ));
        info.setExternalId(externalId);

        // zoneId / providerType
        info.setZoneId(msg.getZoneId());
        info.setProviderType(msg.getProviderType() != null ? String.valueOf(msg.getProviderType()) : "VSPHERE");

        // 스펙
        Integer cpu = firstInt(
                detail.get("cpu_cores"),
                tfVars != null ? tfVars.get("cpu_cores") : null,
                msg.getCpuCores()
        );
        Integer mem = firstInt(
                detail.get("memory_gb"),
                tfVars != null ? tfVars.get("memory_gb") : null,
                msg.getMemoryGb()
        );
        Integer disk = firstInt(
                detail.get("disk_gb"),
                tfVars != null ? tfVars.get("disk_gb") : null,
                msg.getDiskGb()
        );
        info.setCpuCores(cpu);
        info.setMemoryGb(mem);
        info.setDiskGb(disk);

        // OS 타입 (os_image.code 우선)
        info.setOsType(resolveOsType(msg));

        // IP / NIC 주소
        List<String> ipCandidates = new ArrayList<>();
        addIfNotBlank(ipCandidates, asString(detail.get("ip_address")));
        addIfNotBlank(ipCandidates, asString(detail.get("ip")));
        addIfNotBlank(ipCandidates, asString(detail.get("default_ip_address")));
        addCandidatesFromList(ipCandidates, detail.get("guest_ips"));
        addCandidatesFromList(ipCandidates, detail.get("ip_addresses"));

        if (ipCandidates.isEmpty()) {
            // detail에 없으면 전체 output에서 가져오기
            ipCandidates.addAll(extractAllIpsFromOutputs(tfOutputs));
        }

        String primaryIp = choosePreferredIp(ipCandidates);
        info.setIpAddress(primaryIp);
        info.setNicAddresses(buildNicAddresses(ipCandidates));

        return info;
    }

    private ProvisionResultMessage.InstanceInfo buildInstanceInfoFallback(ProvisionJobMessage msg,
                                                                          String baseVmName,
                                                                          int index,
                                                                          List<String> vmIds,
                                                                          List<String> allIps,
                                                                          Map<String, Object> tfVars) {
        ProvisionResultMessage.InstanceInfo info = new ProvisionResultMessage.InstanceInfo();

        info.setName(baseVmNameForIndex(baseVmName, index));

        // externalId
        String extId = null;
        if (vmIds != null && !vmIds.isEmpty()) {
            if (vmIds.size() > index) {
                extId = vmIds.get(index);
            } else {
                extId = vmIds.get(0);
            }
        }
        info.setExternalId(extId);

        info.setZoneId(msg.getZoneId());
        info.setProviderType(msg.getProviderType() != null ? String.valueOf(msg.getProviderType()) : "VSPHERE");

        Integer cpu = firstInt(
                tfVars != null ? tfVars.get("cpu_cores") : null,
                msg.getCpuCores()
        );
        Integer mem = firstInt(
                tfVars != null ? tfVars.get("memory_gb") : null,
                msg.getMemoryGb()
        );
        Integer disk = firstInt(
                tfVars != null ? tfVars.get("disk_gb") : null,
                msg.getDiskGb()
        );
        info.setCpuCores(cpu);
        info.setMemoryGb(mem);
        info.setDiskGb(disk);

        // OS 타입 (os_image.code 우선)
        info.setOsType(resolveOsType(msg));

        // IP / NIC
        List<String> ipCandidates = new ArrayList<>();
        if (allIps != null && !allIps.isEmpty()) {
            if (allIps.size() > index) {
                ipCandidates.add(allIps.get(index));
            } else {
                ipCandidates.addAll(allIps);
            }
        }
        String primaryIp = choosePreferredIp(ipCandidates);
        info.setIpAddress(primaryIp);
        info.setNicAddresses(buildNicAddresses(ipCandidates));

        return info;
    }

    private String baseVmNameForIndex(String baseVmName, int index) {
        if (index == 0) return baseVmName;
        return baseVmName + "-" + (index + 1);
    }

    private List<String> extractVmIdsFromOutputs(Map<String, Object> tfOutputs) {
        if (tfOutputs == null) return Collections.emptyList();

        Object v = firstNonNull(
                tfOutputs.get("worker_vm_ids"),
                tfOutputs.get("vm_ids"),
                tfOutputs.get("vm_id")
        );
        return asStringList(v);
    }

    private List<String> extractAllIpsFromOutputs(Map<String, Object> tfOutputs) {
        if (tfOutputs == null) return Collections.emptyList();
        List<String> result = new ArrayList<>();

        addCandidatesFromList(result, tfOutputs.get("worker_guest_ips"));
        addCandidatesFromList(result, tfOutputs.get("vm_ip_addresses"));
        addCandidatesFromList(result, tfOutputs.get("ip_addresses"));

        Object ip = firstNonNull(
                tfOutputs.get("worker_ip_address"),
                tfOutputs.get("ip_address")
        );
        if (ip != null) {
            String s = asString(ip);
            if (isNotBlank(s)) result.add(s);
        }

        return result;
    }

    private String buildNicAddresses(List<String> ipCandidates) {
        if (ipCandidates == null || ipCandidates.isEmpty()) return null;
        List<String> nic = new ArrayList<>();
        for (String ip : ipCandidates) {
            String norm = normalizeIp(ip);
            if (norm != null && norm.startsWith("172.")) {
                nic.add(norm);
            }
        }
        if (nic.isEmpty()) return null;
        return String.join(",", nic);
    }

    /**
     * osType 결정 로직
     * - 1순위: additionalConfig.os_image_code (BE에서 세팅한 os_image.code)
     * - 2순위: family + version + variant 문자열 (기존 방식)
     */
    private String resolveOsType(ProvisionJobMessage msg) {
        if (msg == null) return null;

        // 1) os_image_code 우선
        Map<String, Object> add = msg.getAdditionalConfig();
        if (add != null) {
            Object codeObj = add.get("os_image_code");
            if (codeObj != null) {
                String code = String.valueOf(codeObj).trim();
                if (!code.isEmpty()) {
                    // 예: ubuntu-22.04-minimal-x86_64 또는 rocky
                    return code;
                }
            }
        }

        // 2) fallback: family + version + variant
        if (msg.getOs() == null) return null;
        ProvisionJobMessage.OsSpec os = msg.getOs();
        StringBuilder sb = new StringBuilder();
        if (isNotBlank(os.getFamily())) sb.append(os.getFamily());
        if (isNotBlank(os.getVersion())) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(os.getVersion());
        }
        if (isNotBlank(os.getVariant())) {
            if (sb.length() > 0) sb.append(" ");
            sb.append(os.getVariant());
        }
        String s = sb.toString().trim();
        return s.isEmpty() ? null : s;
    }

    // ============================================================
    // action 유추 + 에러 힌트
    // ============================================================

    private String resolveAction(Object msg) {
        // ProvisionJobMessage에는 action 필드가 있으므로 우선 활용
        if (msg instanceof ProvisionJobMessage m && isNotBlank(m.getAction())) {
            return m.getAction();
        }

        String fromGetter = reflectString(msg, "getAction", "getOperation", "getOp", "getCommand", "getMode");
        if (isNotBlank(fromGetter)) return fromGetter;

        String fromField = reflectFieldString(msg, "action", "operation", "mode");
        if (isNotBlank(fromField)) return fromField;

        Object req = safeInvoke(msg, "getRequest");
        String fromReq = reflectString(req, "getAction", "getOperation", "getOp", "getCommand", "getMode");
        if (isNotBlank(fromReq)) return fromReq;

        return "apply";
    }

    private RuntimeException wrapTerraformException(RuntimeException e) {
        String msg = e.getMessage() == null ? "" : e.getMessage();
        StringBuilder hint = new StringBuilder();

        if (msg.contains("Invalid single-argument block definition")) {
            hint.append("\n\n[힌트] Terraform 변수 블록은 한 줄에 하나의 속성만 둘 수 없습니다.")
                    .append("\n예) ❌  variable \"x\" { type = string, default = \"\" }")
                    .append("\n    ✅  variable \"x\" {")
                    .append("\n          type    = string")
                    .append("\n          default = \"\"")
                    .append("\n        }");
        }

        if (msg.toLowerCase(Locale.ROOT).contains("no configuration files")) {
            hint.append("\n\n[원인] 작업 디렉토리에 .tf 구성 파일이 없습니다.")
                    .append("\n[조치] 아래 중 하나 설정:")
                    .append("\n  - application.properties: terraform.module.embedded=true")
                    .append("\n  - 또는 terraform.module.source: git::https://...//vsphere-vm?ref=v1.2.3")
                    .append("\n  - 또는 terraform.module.localDir: /path/to/modules (해당 경로에 *.tf 존재)");
        }

        String wrapped = msg + hint;
        return new RuntimeException(wrapped, e);
    }

    private String mergeOutErr(String out, String err) {
        if ((out == null || out.isBlank()) && (err == null || err.isBlank())) return "";
        StringBuilder sb = new StringBuilder("╷\n");
        if (out != null && !out.isBlank()) {
            sb.append("┌─ stdout ─────────────────────────────\n").append(out);
        }
        if (err != null && !err.isBlank()) {
            sb.append("└─ stderr ─────────────────────────────\n").append(err);
        }
        return sb.toString();
    }

    // ============================================================
    // 리플렉션 / 유틸
    // ============================================================

    private Object safeInvoke(Object target, String method) {
        if (target == null) return null;
        try {
            Method m = target.getClass().getMethod(method);
            return m.invoke(target);
        } catch (Exception ignore) {
            return null;
        }
    }

    private String reflectString(Object target, String... methods) {
        if (target == null) return null;
        for (String m : methods) {
            try {
                Method mm = target.getClass().getMethod(m);
                Object v = mm.invoke(target);
                if (v != null) return String.valueOf(v);
            } catch (Exception ignore) {
            }
        }
        return null;
    }

    private String reflectFieldString(Object target, String... fields) {
        if (target == null) return null;
        for (String f : fields) {
            try {
                Field field = target.getClass().getDeclaredField(f);
                field.setAccessible(true);
                Object v = field.get(target);
                if (v != null) return String.valueOf(v);
            } catch (Exception ignore) {
            }
        }
        return null;
    }

    private long parseJobId(Object v) {
        if (v == null) return System.currentTimeMillis();
        try {
            if (v instanceof Number n) return n.longValue();
            return Long.parseLong(String.valueOf(v));
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    private Long toLong(Object v) {
        if (v == null) return null;
        try {
            if (v instanceof Number n) return n.longValue();
            return Long.parseLong(String.valueOf(v));
        } catch (Exception e) {
            return null;
        }
    }

    private String nvl(String s, String def) {
        return (s == null || s.isBlank()) ? def : s;
    }

    private boolean isNotBlank(String s) {
        return s != null && !s.isBlank();
    }

    private int safe(Number v, int def) {
        return v == null ? def : v.intValue();
    }

    private String asString(Object v) {
        return v == null ? null : String.valueOf(v);
    }

    private String firstLine(String s) {
        if (s == null) return "";
        int i = s.indexOf('\n');
        return i >= 0 ? s.substring(0, i) : s;
    }

    private void addIfNotBlank(List<String> list, String value) {
        if (isNotBlank(value)) list.add(value);
    }

    private Object firstNonNull(Object... candidates) {
        if (candidates == null) return null;
        for (Object c : candidates) {
            if (c != null) return c;
        }
        return null;
    }

    private Integer firstInt(Object... candidates) {
        if (candidates == null) return null;
        for (Object c : candidates) {
            if (c == null) continue;
            if (c instanceof Number n) return n.intValue();
            try {
                return Integer.parseInt(String.valueOf(c));
            } catch (Exception ignore) {
            }
        }
        return null;
    }

    private List<String> asStringList(Object v) {
        List<String> result = new ArrayList<>();
        if (v == null) return result;
        if (v instanceof List<?> list) {
            for (Object o : list) {
                if (o != null) result.add(String.valueOf(o));
            }
        } else {
            result.add(String.valueOf(v));
        }
        return result;
    }

    // ============================================================
    // IP 선택 유틸
    // ============================================================

    private void addCandidatesFromList(List<String> target, Object listObj) {
        if (!(listObj instanceof List<?> list)) return;
        for (Object o : list) {
            String ip = normalizeIp(asString(o));
            if (ip != null && !target.contains(ip)) {
                target.add(ip);
            }
        }
    }

    /**
     * 여러 후보 IP 중 내부망 IP를 우선해서 선택
     */
    private String choosePreferredIp(List<String> candidates) {
        if (candidates == null || candidates.isEmpty()) return null;

        List<String> filtered = new ArrayList<>();
        for (String c : candidates) {
            String ip = normalizeIp(c);
            if (ip != null && !filtered.contains(ip)) {
                filtered.add(ip);
            }
        }
        if (filtered.isEmpty()) return null;

        String prefixEnv = System.getenv("INTERNAL_IP_PREFIX"); // 예: "172.16."
        if (prefixEnv != null && !prefixEnv.isBlank()) {
            for (String ip : filtered) {
                if (ip.startsWith(prefixEnv)) return ip;
            }
        }

        for (String ip : filtered) {
            if (ip.startsWith("172.16.")) return ip;
        }
        for (String ip : filtered) {
            if (ip.startsWith("172.")) return ip;
        }
        for (String ip : filtered) {
            if (ip.startsWith("10.")) return ip;
        }

        return filtered.get(0);
    }

    private String normalizeIp(String ip) {
        if (ip == null) return null;
        ip = ip.trim();
        if (ip.isEmpty()) return null;
        if ("null".equalsIgnoreCase(ip) || "none".equalsIgnoreCase(ip)) return null;
        return ip;
    }

    // tfvars(JSON) 파일 쓰기
    private void writeVarsFile(File file, Map<String, Object> vars) {
        try (FileOutputStream fos = new FileOutputStream(file);
             OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {

            om.writeValue(osw, vars);

        } catch (IOException e) {
            throw new RuntimeException("Failed to write tfvars file: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * additionalConfig에서 tfRunId 추출
     */
    private Long extractTfRunId(Map<String, Object> additionalConfig) {
        if (additionalConfig == null) return null;

        Object tfRunIdObj = additionalConfig.get("tfRunId");
        if (tfRunIdObj == null) return null;

        try {
            if (tfRunIdObj instanceof Number n) {
                return n.longValue();
            }
            return Long.parseLong(String.valueOf(tfRunIdObj));
        } catch (Exception e) {
            log.warn("Failed to parse tfRunId: {}", tfRunIdObj);
            return null;
        }
    }

    /**
     * additionalConfig에서 stateUri 추출
     */
    private String extractStateUri(Map<String, Object> additionalConfig) {
        if (additionalConfig == null) return null;

        Object stateUriObj = additionalConfig.get("stateUri");
        if (stateUriObj == null) return null;

        String stateUri = String.valueOf(stateUriObj).trim();
        return stateUri.isEmpty() || "null".equalsIgnoreCase(stateUri) ? null : stateUri;
    }

    /**
     * 원본 ProvisionJobMessage.additionalConfig 에서 vmId 추출
     */
    private String extractVmIdFromMsg(ProvisionJobMessage msg) {
        if (msg == null) return null;
        Map<String, Object> add = msg.getAdditionalConfig();
        if (add == null) return null;
        Object vmIdObj = add.get("vmId");
        return vmIdObj != null ? String.valueOf(vmIdObj) : null;
    }
}
