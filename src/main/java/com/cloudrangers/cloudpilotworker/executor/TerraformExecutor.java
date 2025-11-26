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

    public String execute(ProvisionJobMessage msg, boolean isFinalAttempt) {  // ⭐️ 파라미터 추가
        Objects.requireNonNull(msg, "ProvisionJobMessage must not be null");

        Object rawJobId = safeInvoke(msg, "getJobId");
        String jobIdStr = rawJobId != null ? String.valueOf(rawJobId) : String.valueOf(System.currentTimeMillis());
        long jobId = parseJobId(rawJobId);

        currentJobId.set(jobIdStr);

        // 로그 정제를 위한 버퍼 및 컨텍스트 초기화
        refinedLogBuffer.set(new StringBuilder());
        rawLogBuffer.set(new StringBuilder());
        TerraformLogContext context = new TerraformLogContext();
        context.setJobId(jobIdStr);
        logContext.set(context);

        try {
            String vmName = nvl(msg.getVmName(), "vm-" + jobId);
            String action = resolveAction(msg);

            // 작업 디렉토리 생성
            File workDir = new File("/tmp/terraform/" + jobIdStr);
            if (!workDir.exists() && !workDir.mkdirs()) {
                throw new RuntimeException("Failed to create workDir: " + workDir.getAbsolutePath());
            }

            Map<String, Object> tfVars = buildTfVarsFromMessage(msg, vmName);
            Map<String, Object> tfOutputs = Collections.emptyMap();

            try {
                // 실제 Terraform 실행
                execute(jobId, vmName, action, workDir, tfVars);

                // apply인 경우에만 Output 읽기 및 성공 이벤트 전송
                if ("apply".equalsIgnoreCase(action)) {
                    Map<String, String> env = new HashMap<>();
                    env.put("TF_IN_AUTOMATION", "1");

                    // Output 읽기
                    tfOutputs = readTerraformOutputs(workDir, env);

                    // SUCCESS Event 전송 (InstanceInfo 포함)
                    sendSuccessEvent(jobIdStr, msg, vmName, tfVars, tfOutputs);
                }

                return vmName;
            } catch (RuntimeException e) {
                // 실패 시 ERROR 이벤트 전송
                sendErrorEvent(jobIdStr, e);
                throw e;
            }
        } finally {
            // ⭐️ 마지막 시도일 때만 refined 로그 저장
            try {
                String refinedLog = refinedLogBuffer.get().toString();
                String rawLog = rawLogBuffer.get().toString();
                logStorageService.saveLogsToLocal(jobIdStr, "terraform", refinedLog, rawLog, isFinalAttempt);
            } catch (Exception e) {
                log.error("Failed to save logs to local filesystem for jobId: {}", jobIdStr, e);
            }

            // ThreadLocal 정리
            currentJobId.remove();
            refinedLogBuffer.remove();
            rawLogBuffer.remove();
            logContext.remove();
        }
    }

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

        if (tfVars != null && !tfVars.isEmpty()) {
            File vars = new File(workDir, VARS_FILE_NAME);
            writeVarsFile(vars, tfVars);
            log.info("Wrote vars file: {}", vars.getAbsolutePath());
        }

        runTerraformSequence(workDir, env, action);
    }

    public void execute(File workDir, String action) {
        Map<String, String> env = Map.of("TF_IN_AUTOMATION", "1");
        ensureModulePresent(workDir, env);
        runTerraformSequence(workDir, env, action);
    }

    // ============================================================
    // ⭐️ [IP 추출] (WorkerListener에서 호출)
    //  - z_automation_outputs.tf에 정의된 output 사용
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

            // 1. worker_guest_ips (자동 생성 Output)에서 172.16.* 찾기
            if (outputs.containsKey("worker_guest_ips")) {
                Object val = outputs.get("worker_guest_ips").get("value");
                if (val instanceof List) {
                    List<?> ips = (List<?>) val;
                    for (Object obj : ips) {
                        String ip = String.valueOf(obj);
                        if (ip.startsWith("172.16.")) {
                            log.info("[Terraform] Found Internal IP from worker_guest_ips: {}", ip);
                            return ip;
                        }
                    }
                }
            }

            // 2. worker_ip_address (Fallback)
            if (outputs.containsKey("worker_ip_address")) {
                String ip = String.valueOf(outputs.get("worker_ip_address").get("value"));
                if (ip.startsWith("172.16.")) return ip;
            }

            // 3. vm_ip_addresses (기존 Output)
            if (outputs.containsKey("vm_ip_addresses")) {
                Object val = outputs.get("vm_ip_addresses").get("value");
                if (val instanceof List) {
                    List<?> ips = (List<?>) val;
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
    // ⭐️ 모듈 스테이징 & 자동 Output 파일 생성 (V1 + V2 병합)
    // ============================================================

    private void ensureModulePresent(File dir, Map<String, String> env) {
        log.debug("=== ensureModulePresent START ===");
        log.debug("Working directory: {}", dir.getAbsolutePath());
        log.debug("useEmbeddedModule: {}", useEmbeddedModule);
        log.debug("moduleResourcePath: '{}'", moduleResourcePath);

        // 이미 .tf 파일 있을 경우 -> 모듈은 있다고 보고 automation outputs만 보장
        if (hasTfFiles(dir)) {
            log.info("✓ Terraform config already present in {}", dir.getAbsolutePath());
            generateExtraOutputsFile(dir);
            return;
        }

        log.info("No .tf files found. Starting module staging...");

        // 1) Embedded module (classpath)
        if (useEmbeddedModule && isNotBlank(moduleResourcePath)) {
            log.info("✓ Using embedded module from classpath: {}", moduleResourcePath);
            try {
                stageModuleFromClasspath(dir);
                generateExtraOutputsFile(dir);
                return;
            } catch (Exception e) {
                log.error("Failed to stage embedded module", e);
                throw new RuntimeException("Failed to stage embedded module from classpath: " + moduleResourcePath, e);
            }
        }

        // 2) Remote module source (TERRAFORM_MODULE_SOURCE or property)
        String src = normalizeEmptyToNull(moduleSourceProp);
        if (src == null) {
            src = normalizeEmptyToNull(System.getenv("TERRAFORM_MODULE_SOURCE"));
        }
        log.debug("Resolved module source: '{}'", src);

        if (src != null) {
            log.info("✓ Using module source: {}", src);
            stageModuleFromSource(dir, env, src);
            generateExtraOutputsFile(dir);
            return;
        }

        // 3) Local directory
        String localDir = normalizeEmptyToNull(moduleLocalDir);
        log.debug("Checking local module directory: '{}'", localDir);

        if (localDir != null) {
            Path localPath = Path.of(localDir);
            if (Files.exists(localPath) && Files.isDirectory(localPath)) {
                log.info("✓ Using local module directory: {}", localDir);
                stageModuleFromLocal(dir, env, localPath);
                generateExtraOutputsFile(dir);
                return;
            } else {
                log.warn("✗ Local module directory not found or not a directory: {}", localDir);
            }
        }

        log.error("✗ No Terraform module source configured!");
        throw new RuntimeException("No Terraform configuration found and no module source configured.");
    }

    // z_automation_outputs.tf 자동 생성 (worker_ip_address / worker_guest_ips / worker_vm_ids)
    private void generateExtraOutputsFile(File dir) {
        File outputFile = new File(dir, "z_automation_outputs.tf");
        try (FileWriter writer = new FileWriter(outputFile, false)) {
            writer.write("""
                # === Auto-generated by Worker for Automation ===
                
                output "worker_ip_address" {
                  value       = vsphere_virtual_machine.vm[0].default_ip_address
                  description = "Worker internal use: Default IP"
                }
                
                # All guest IP addresses
                output "worker_guest_ips" {
                  value       = vsphere_virtual_machine.vm[0].guest_ip_addresses
                  description = "Worker internal use: All Guest IPs"
                }
                
                output "worker_vm_ids" {
                  value = vsphere_virtual_machine.vm[*].id
                }
                """);
            log.info("✓ Created z_automation_outputs.tf for IP extraction.");
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate extra outputs file", e);
        }
    }

    // ===== Module Staging Helpers =====

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

        log.info("✓ Extracted {} Terraform files", resources.length);

        if (!hasTfFiles(targetDir)) {
            throw new IOException("Extraction completed but no .tf files found in " + targetDir);
        }
    }

    private void stageModuleFromSource(File dir, Map<String, String> env, String source) {
        log.info("Staging module via 'terraform init -from-module={}'", source);

        try {
            // 원격 모듈 다운로드 + 기본 init
            executeCommand(dir, env, "terraform", "init", "-from-module=" + source, "-input=false", "-no-color");
            log.info("✓ Module staged from source");

            // provider init
            executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
            log.info("✓ Providers initialized");

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
            log.info("✓ Module files copied");

            if (!hasTfFiles(dir)) {
                throw new RuntimeException("Copied from local directory but no .tf files present");
            }

            executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
            log.info("✓ Providers initialized");

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

        if (env != null && !env.isEmpty()) pb.environment().putAll(env);

        try {
            Process p = pb.start();

            StringBuilder stdoutBuf = new StringBuilder();
            StringBuilder stderrBuf = new StringBuilder();
            CountDownLatch latch = new CountDownLatch(2);

            // === 로그 정제를 위한 변수 (Thread 생성 전에 가져오기) ===
            TerraformLogContext ctx = logContext.get();
            StringBuilder refinedBuffer = refinedLogBuffer.get();
            StringBuilder rawBuffer = rawLogBuffer.get();

            Thread tOut = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;

                    while ((line = br.readLine()) != null) {
                        stdoutBuf.append(line).append('\n');

                        // 원본 로그 저장
                        if (rawBuffer != null) {
                            rawBuffer.append(line).append('\n');
                        }

                        // 로그 정제
                        if (ctx != null) {
                            String refinedLine = logRefiner.refineLine(line, ctx);
                            if (refinedLine != null && refinedBuffer != null) {
                                refinedBuffer.append(refinedLine).append('\n');
                                log.info("[terraform-refined] {}", refinedLine);
                            }
                        }

                        // 기존 로그도 유지
                        log.info("[terraform] {}", line);
                        sendLogEvent(jobId, step, line);
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

                        // 원본 로그 저장
                        if (rawBuffer != null) {
                            rawBuffer.append("[stderr] ").append(line).append('\n');
                        }

                        // 로그 정제 (에러는 더 중요)
                        if (ctx != null) {
                            String refinedLine = logRefiner.refineLine(line, ctx);
                            if (refinedLine != null && refinedBuffer != null) {
                                refinedBuffer.append(refinedLine).append('\n');
                                log.error("[terraform-refined-error] {}", refinedLine);
                            }
                        }

                        // 기존 로그도 유지
                        log.error("[terraform] {}", line);
                        sendLogEvent(jobId, step, line);
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
                sendLogEvent(jobId, step, msg);
                throw new RuntimeException(msg);
            }

            latch.await(5, TimeUnit.SECONDS);

            int exit = p.exitValue();
            if (exit != 0) {
                String merged = mergeOutErr(stdoutBuf.toString(), stderrBuf.toString());
                String msg = "Command failed: " + display + " (exit " + exit + ")\n" + merged;
                sendLogEvent(jobId, step, firstLine(msg));
                throw wrapTerraformException(new RuntimeException(msg));
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            String msg = "Command failed to start/run: " + display + " - " + e.getMessage();
            sendLogEvent(jobId, step, firstLine(msg));
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
     * Terraform 전체 시퀀스가 정상 종료된 뒤 SUCCESS 이벤트 전송.
     * - vm_details / vm_ip_addresses / ip_address / tfVars 등을 조합해 InstanceInfo 채움
     * - 내부망(172/10) 우선 IP 선택 + NIC 리스트 수집
     */
    private void sendSuccessEvent(String jobId,
                                  ProvisionJobMessage msg,
                                  String vmName,
                                  Map<String, Object> tfVars,
                                  Map<String, Object> tfOutputs) {
        if (jobId == null) return;
        if (tfOutputs == null) tfOutputs = Collections.emptyMap();

        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.SUCCESS);
            result.setStatus("SUCCEEDED");
            result.setStep("terraform_apply");
            result.setTimestamp(OffsetDateTime.now());

            // VM 개수 추론
            int vmCount = 1;
            Object vmCountObj = tfVars != null ? tfVars.get("vm_count") : null;
            if (vmCountObj instanceof Number n) {
                vmCount = Math.max(1, n.intValue());
            } else {
                Object v = safeInvoke(msg, "getVmCount");
                if (v instanceof Number n2) {
                    vmCount = Math.max(1, n2.intValue());
                } else if (v != null) {
                    try {
                        vmCount = Math.max(1, Integer.parseInt(String.valueOf(v)));
                    } catch (Exception ignore) {
                    }
                }
            }

            Long zoneId = toLong(safeInvoke(msg, "getZoneId"));
            String providerType = reflectString(msg, "getProviderType", "getProvider");

            Integer cpuCores = null;
            Integer memoryGb = null;
            Integer diskGb = null;

            Object oCpu = safeInvoke(msg, "getCpuCores");
            if (oCpu instanceof Number n) cpuCores = n.intValue();
            Object oMem = safeInvoke(msg, "getMemoryGb");
            if (oMem instanceof Number n2) memoryGb = n2.intValue();
            Object oDisk = safeInvoke(msg, "getDiskGb");
            if (oDisk instanceof Number n3) diskGb = n3.intValue();

            // terraform output에서 vm_details / vm_ip_addresses / ip_address 파싱
            List<Map<String, Object>> vmDetails = extractVmDetails(tfOutputs);

            List<?> vmIpAddressesRaw = null;
            Object vmIpListObj = tfOutputs.get("vm_ip_addresses");
            if (vmIpListObj instanceof List<?> list) {
                vmIpAddressesRaw = list;
            }

            List<String> vmIpAddresses = null;
            if (vmIpAddressesRaw != null) {
                vmIpAddresses = new ArrayList<>();
                for (Object o : vmIpAddressesRaw) {
                    vmIpAddresses.add(normalizeIp(asString(o)));
                }
            }

            String singleIp = normalizeIp(asString(tfOutputs.get("ip_address")));

            List<ProvisionResultMessage.InstanceInfo> instances = new ArrayList<>();

            for (int i = 0; i < vmCount; i++) {
                ProvisionResultMessage.InstanceInfo info = new ProvisionResultMessage.InstanceInfo();

                String name = (vmCount == 1) ? vmName : vmName + "-" + (i + 1);
                String externalId = null;

                // IP 후보 리스트
                List<String> ipCandidates = new ArrayList<>();

                // ===========================
                // vm_details 기반 정보
                // ===========================
                Map<String, Object> detail = null;
                if (!vmDetails.isEmpty() && i < vmDetails.size()) {
                    detail = vmDetails.get(i);

                    String detailName = asString(detail.get("name"));
                    if (isNotBlank(detailName)) {
                        name = detailName;
                    }

                    String moid = asString(detail.getOrDefault("moid", detail.get("id")));
                    externalId = moid;

                    // CPU
                    Object cpuVal = detail.get("cpu_cores");
                    if (cpuVal instanceof Number n) cpuCores = n.intValue();

                    // Memory (MB → GB)
                    Object memMbVal = detail.get("memory_mb");
                    if (memMbVal instanceof Number n) memoryGb = Math.max(1, n.intValue() / 1024);

                    // Disk
                    Object diskGbVal = detail.get("total_disk_gb");
                    if (diskGbVal instanceof Number n) diskGb = n.intValue();

                    // IP 후보: ip_address
                    String defaultIp = normalizeIp(asString(detail.get("ip_address")));
                    if (defaultIp != null) {
                        ipCandidates.add(defaultIp);
                    }

                    // IP 후보: guest_ip_addresses
                    addCandidatesFromList(ipCandidates, detail.get("guest_ip_addresses"));
                }

                // ===========================
                // vm_ip_addresses 기반 정보
                // ===========================
                if (vmIpAddresses != null && i < vmIpAddresses.size()) {
                    String ip = normalizeIp(vmIpAddresses.get(i));
                    if (ip != null) ipCandidates.add(ip);
                }

                // 단일 출력(singleIp) – 단일 VM일 때만 후보에 추가
                if (singleIp != null && vmCount == 1) {
                    ipCandidates.add(singleIp);
                }

                // tfvars 기반 STATIC IP
                String ipFromTfVars = normalizeIp(asString(tfVars.get("ipv4_address")));
                if (ipFromTfVars != null) {
                    ipCandidates.add(ipFromTfVars);
                }

                // 요청 바디 기반 STATIC IP
                Object netObj = safeInvoke(msg, "getNet");
                if (netObj != null) {
                    Object ipv4Obj = safeInvoke(netObj, "getIpv4");
                    if (ipv4Obj != null) {
                        String ip = normalizeIp(reflectString(ipv4Obj, "getAddress", "getIp"));
                        if (ip != null) {
                            ipCandidates.add(ip);
                        }
                    }
                }

                // ===========================
                // 기본 Public IP / 내부망 우선 선택
                // ===========================
                String chosenIp = choosePreferredIp(ipCandidates);

                // ===========================
                // NIC 전체 리스트 생성 (IPv4 전용, 172/10 대역 수집)
                // ===========================
                List<String> nicAddresses = new ArrayList<>();
                if (detail != null) {
                    Object guestIpObj = detail.get("guest_ip_addresses");
                    if (guestIpObj instanceof List<?> rawList) {
                        for (Object ipObj : rawList) {
                            String ip = normalizeIp(asString(ipObj));
                            if (ip != null
                                    && ip.matches("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b")
                                    && (ip.startsWith("172.") || ip.startsWith("10."))) {
                                nicAddresses.add(ip);
                            }
                        }
                    }
                }

                String nicAddressesStr = nicAddresses.isEmpty()
                        ? null
                        : String.join(",", nicAddresses);

                // ===========================
                // 최종 팀 네트워크 IP 선택
                // 1) nicAddresses(내부망 후보)에 대해 choosePreferredIp 적용
                // 2) 없으면 chosenIp 사용
                // ===========================
                String teamIp = choosePreferredIp(nicAddresses);
                String ipForInstance = (teamIp != null) ? teamIp : chosenIp;

                // ===========================
                // info 필드 설정
                // ===========================
                info.setExternalId(externalId);
                info.setIpAddress(ipForInstance);      // vm_instance.ip 로 저장
                info.setNicAddresses(nicAddressesStr); // tags.nicAddresses 참고용

                info.setName(name);
                info.setZoneId(zoneId);
                info.setProviderType(providerType);
                info.setCpuCores(cpuCores);
                info.setMemoryGb(memoryGb);
                info.setDiskGb(diskGb);
                info.setOsType(null);

                log.info("[TerraformExecutor] VM index={}, name={}, externalId={}, chosenIp={}, teamIp={}, finalIp={}, nicAddresses={}",
                        i, name, externalId, chosenIp, teamIp, ipForInstance, nicAddressesStr);

                instances.add(info);
            }

            result.setInstances(instances);

            if (!instances.isEmpty()) {
                ProvisionResultMessage.InstanceInfo first = instances.get(0);
                String vmId = isNotBlank(first.getExternalId()) ? first.getExternalId() : first.getName();
                result.setVmId(vmId);
            }

            result.setMessage("Terraform apply succeeded (" + vmCount + " VM(s))");

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                return m;
            });

            log.info("[TerraformExecutor] Sent SUCCESS event for jobId={}, vmCount={}, outputsKeys={}",
                    jobId, vmCount, tfOutputs.keySet());
        } catch (Exception e) {
            log.warn("[TerraformExecutor] Failed to send SUCCESS event for jobId={}: {}", jobId, e.getMessage());
        }
    }

    /**
     * Terraform 실행 과정에서 예외가 발생했을 때 ERROR 이벤트 전송.
     */
    private void sendErrorEvent(String jobId, RuntimeException e) {
        if (jobId == null) return;

        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.ERROR);
            result.setStatus("FAILED");
            result.setStep("terraform");
            result.setTimestamp(OffsetDateTime.now());
            result.setMessage(firstLine(e.getMessage()));

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                return m;
            });

            log.warn("[TerraformExecutor] Sent ERROR event for jobId={}: {}", jobId, firstLine(e.getMessage()));
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

        if (env != null && !env.isEmpty()) pb.environment().putAll(env);

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

        // === vSphere 연결 정보 (환경 변수에서) ===
        tf.put("vsphere_server", nvl(System.getenv("VSPHERE_SERVER"), "vcenter.fisa.com"));
        tf.put("vsphere_user", nvl(System.getenv("VSPHERE_USER"), "administrator@fisa.ce5"));
        tf.put("vsphere_password", System.getenv("VSPHERE_PASSWORD"));
        tf.put("allow_unverified_ssl", Boolean.parseBoolean(nvl(System.getenv("VSPHERE_ALLOW_UNVERIFIED_SSL"), "true")));

        // === vSphere 리소스 위치 ===
        Object datacenterObj = safeInvoke(msg, "getDatacenter");
        tf.put("datacenter", nvl(
                datacenterObj != null ? String.valueOf(datacenterObj) : null,
                nvl(System.getenv("VSPHERE_DATACENTER"), "ce5-3")
        ));

        Object clusterObj = safeInvoke(msg, "getCluster");
        tf.put("cluster", nvl(
                clusterObj != null ? String.valueOf(clusterObj) : null,
                nvl(System.getenv("VSPHERE_CLUSTER"), "")
        ));

        Object datastoreObj = safeInvoke(msg, "getDatastore");
        tf.put("datastore", nvl(
                datastoreObj != null ? String.valueOf(datastoreObj) : null,
                nvl(System.getenv("VSPHERE_DATASTORE"), "HDD1 (1)")
        ));

        Object folderObj = safeInvoke(msg, "getFolder");
        tf.put("folder", nvl(
                folderObj != null ? String.valueOf(folderObj) : null,
                ""
        ));

        // === VM 설정 ===
        tf.put("vm_name", vmName);
        tf.put("vm_count", safe((Number) safeInvoke(msg, "getVmCount"), 1));

        // Template / 추가 설정
        Object addCfg = safeInvoke(msg, "getAdditionalConfig");
        Map<String, Object> add = (addCfg instanceof Map) ? (Map<String, Object>) addCfg : Collections.emptyMap();

        String templateName = asString(add.get("templateName"));
        if (!isNotBlank(templateName)) {
            Object templateObj = safeInvoke(msg, "getTemplate");
            if (templateObj != null) {
                templateName = reflectString(templateObj, "getItemName");
            }
        }
        tf.put("template_name", templateName);

        // 리소스 스펙
        tf.put("cpu_cores", safe((Number) safeInvoke(msg, "getCpuCores"), 2));
        tf.put("memory_gb", safe((Number) safeInvoke(msg, "getMemoryGb"), 4));
        tf.put("disk_gb", safe((Number) safeInvoke(msg, "getDiskGb"), 50));

        // 디스크 프로비저닝
        String diskProv = asString(add.getOrDefault("diskProvisioning", "thin"));
        tf.put("disk_provisioning", diskProv.toLowerCase());

        // === 네트워크 설정 ===
        Long teamId = toLong(safeInvoke(msg, "getTeamId"));
        Long zoneId = toLong(safeInvoke(msg, "getZoneId"));

        List<String> networks = resolveNetworkNames(msg, add, teamId, zoneId);
        if (!networks.isEmpty()) {
            // 모듈은 1개만 사용하지만, 추가 NIC가 필요하면 extra_networks로 활용 가능
            tf.put("network", networks.get(0));
            if (networks.size() > 1) {
                tf.put("extra_networks", networks.subList(1, networks.size()));
            }
        } else {
            String defaultNet = normalizeEmptyToNull(System.getenv("VSPHERE_NETWORK"));
            if (defaultNet == null) {
                // 최후의 안전장치: PG-WAN
                defaultNet = "PG-WAN";
            }
            tf.put("network", defaultNet);
        }

        String ipMode = asString(add.getOrDefault("ipAllocationMode", "DHCP"));
        tf.put("ip_allocation_mode", ipMode.toUpperCase());

        // net 객체에서 IP 정보 가져오기
        Object netObj = safeInvoke(msg, "getNet");
        if (netObj != null) {
            Object ipv4Obj = safeInvoke(netObj, "getIpv4");
            if (ipv4Obj != null) {
                String ipv4 = reflectString(ipv4Obj, "getAddress", "getIp");
                if (isNotBlank(ipv4)) {
                    tf.put("ipv4_address", ipv4);

                    Number netmask = (Number) safeInvoke(ipv4Obj, "getNetmask");
                    if (netmask != null) {
                        tf.put("ipv4_netmask", netmask.intValue());
                    }

                    String gateway = reflectString(ipv4Obj, "getGateway");
                    if (isNotBlank(gateway)) {
                        tf.put("ipv4_gateway", gateway);
                    }
                }
            }

            Object dnsObj = safeInvoke(netObj, "getDns");
            if (dnsObj != null && dnsObj instanceof List) {
                List<?> dnsList = (List<?>) dnsObj;
                if (!dnsList.isEmpty()) {
                    List<String> dnsServers = new ArrayList<>();
                    for (Object dns : dnsList) {
                        dnsServers.add(String.valueOf(dns));
                    }
                    tf.put("dns_servers", dnsServers);
                }
            }
        }

        tf.put("domain", "local");

        Object tagsObj = safeInvoke(msg, "getTags");
        if (tagsObj instanceof Map) {
            tf.put("tags", tagsObj);
        } else {
            tf.put("tags", Collections.emptyMap());
        }

        return tf;
    }

    /**
     * 팀/존/요청 additionalConfig를 기반으로 vSphere 네트워크 목록 결정
     * 우선순위:
     *  1) additionalConfig.networks / additionalConfig.network
     *  2) ProvisionJobMessage.getNetwork()
     *  3) teamId 기반 하드코딩 매핑 (예: 1 -> VLAN101_TeamA)
     *  4) VSPHERE_NETWORK 환경변수, 없으면 PG-WAN
     */
    private List<String> resolveNetworkNames(ProvisionJobMessage msg,
                                             Map<String, Object> additionalConfig,
                                             Long teamId,
                                             Long zoneId) {
        List<String> networks = new ArrayList<>();

        // 1) additionalConfig.networks
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

        // 2) additionalConfig.network
        if (networks.isEmpty() && additionalConfig != null) {
            Object networkFromReq = additionalConfig.get("network");
            if (networkFromReq != null) {
                String s = String.valueOf(networkFromReq).trim();
                if (isNotBlank(s)) {
                    networks.add(s);
                }
            }
        }

        // 3) ProvisionJobMessage.getNetwork()
        if (networks.isEmpty()) {
            Object networkObj = safeInvoke(msg, "getNetwork");
            if (networkObj != null) {
                String base = String.valueOf(networkObj).trim();
                if (isNotBlank(base)) {
                    networks.add(base);
                }
            }
        }

        // 4) teamId 기반 하드코딩 매핑
        if (teamId != null) {
            if (teamId == 1L) {
                String teamVlan = "VLAN101_TeamA";
                if (!networks.contains(teamVlan)) {
                    networks.add(0, teamVlan);
                }
                log.info("[TerraformExecutor] teamId={} → network='{}'", teamId, teamVlan);
            }
            // 필요 시 teamId 2,3... 매핑 추가 가능
        }

        // 5) 글로벌 기본값: VSPHERE_NETWORK / PG-WAN
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
    // action 유추 + 에러 힌트
    // ============================================================

    private String resolveAction(Object msg) {
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
            if (v instanceof Number) return ((Number) v).longValue();
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
     * 여러 후보 IP 중에서 "내부망"을 우선해서 고른다.
     * - 환경변수 INTERNAL_IP_PREFIX 가 있으면 그걸 우선 사용 (예: "172.16.")
     * - 없으면 172.16.* → 172.* → 10.* → 그 외 순으로 선택
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

        // 172.16.* 우선
        for (String ip : filtered) {
            if (ip.startsWith("172.16.")) return ip;
        }
        // 172.* 그 외
        for (String ip : filtered) {
            if (ip.startsWith("172.")) return ip;
        }
        // 10.*
        for (String ip : filtered) {
            if (ip.startsWith("10.")) return ip;
        }

        // 그래도 없으면 첫 번째
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

            // ObjectMapper(om)를 사용해서 JSON으로 저장
            om.writeValue(osw, vars);

        } catch (IOException e) {
            throw new RuntimeException("Failed to write tfvars file: " + file.getAbsolutePath(), e);
        }
    }

}
