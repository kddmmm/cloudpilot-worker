package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.dto.ProvisionResultMessage;
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

    // 현재 쓰레드에서 실행 중인 jobId (로그 이벤트에 사용)
    private final ThreadLocal<String> currentJobId = new ThreadLocal<>();

    public TerraformExecutor(ResourceLoader resourceLoader,
                             RabbitTemplate rabbitTemplate,
                             ObjectMapper objectMapper) {
        this.resourceLoader = resourceLoader;
        this.rabbitTemplate = rabbitTemplate;
        this.om = objectMapper.copy().enable(SerializationFeature.INDENT_OUTPUT);
    }

    // ============================================================
    // 1. 외부 진입점 (Execute)
    // ============================================================

    public String execute(ProvisionJobMessage msg) {
        Objects.requireNonNull(msg, "ProvisionJobMessage must not be null");

        Object rawJobId = safeInvoke(msg, "getJobId");
        String jobIdStr = rawJobId != null ? String.valueOf(rawJobId) : String.valueOf(System.currentTimeMillis());
        long jobId = parseJobId(rawJobId);

        currentJobId.set(jobIdStr);
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

                    // V2의 상세한 Success Event 전송 (InstanceInfo 포함)
                    sendSuccessEvent(jobIdStr, msg, vmName, tfVars, tfOutputs);
                }

                return vmName;
            } catch (RuntimeException e) {
                // 실패 시 ERROR 이벤트 전송
                sendErrorEvent(jobIdStr, e);
                throw e;
            }
        } finally {
            currentJobId.remove();
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

        // ⭐️ 모듈 준비 (V1: generateExtraOutputsFile 포함됨)
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
    // ⭐️ [IP 추출] V1 기능 유지 (WorkerListener에서 호출)
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

            // 1. worker_guest_ips (자동 생성된 Output)에서 172.16. 찾기
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

            // 4. 최후의 수단: 그냥 잡히는 IP라도 반환
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

        if (hasTfFiles(dir)) {
            log.info("✓ Terraform config already present in {}", dir.getAbsolutePath());
            // 파일이 이미 있어도 Output 파일은 무조건 재생성 (안전장치 - V1 기능)
            generateExtraOutputsFile(dir);
            return;
        }

        boolean staged = false;

        // 1. Embedded
        if (useEmbeddedModule && isNotBlank(moduleResourcePath)) {
            log.info("✓ Using embedded module from classpath: {}", moduleResourcePath);
            try {
                stageModuleFromClasspath(dir);
                staged = true;
            } catch (Exception e) {
                log.error("Failed to stage embedded module", e);
            }
        }

        // 2. Source URL
        if (!staged) {
            String src = normalizeEmptyToNull(moduleSourceProp);
            if (src == null) src = normalizeEmptyToNull(System.getenv("TERRAFORM_MODULE_SOURCE"));
            if (src != null) {
                log.info("✓ Using module source: {}", src);
                stageModuleFromSource(dir, env, src);
                staged = true;
            }
        }

        // 3. Local Dir
        if (!staged) {
            String localDir = normalizeEmptyToNull(moduleLocalDir);
            if (localDir != null) {
                Path localPath = Path.of(localDir);
                if (Files.exists(localPath) && Files.isDirectory(localPath)) {
                    log.info("✓ Using local module directory: {}", localDir);
                    stageModuleFromLocal(dir, env, localPath);
                    staged = true;
                }
            }
        }

        if (!staged) {
            log.error("✗ No Terraform module source configured!");
            throw new RuntimeException("No Terraform configuration found and no module source configured.");
        }

        // ⭐️ [핵심] 모듈 복사 완료 후, Worker 전용 Output 파일 생성 (V1 기능)
        generateExtraOutputsFile(dir);

        // Init 실행
        executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
    }

    // ⭐️ [V1] Worker 전용 Output 파일 생성 (Automation을 위해 필수)
    private void generateExtraOutputsFile(File dir) {
        File outputFile = new File(dir, "z_automation_outputs.tf");
        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write("""
                # === Auto-generated by Worker for Automation ===
                
                output "worker_ip_address" {
                  value       = vsphere_virtual_machine.vm[0].default_ip_address
                  description = "Worker internal use: Default IP"
                }
                
                # ⭐️ 모든 IP 목록을 가져오도록 추가
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

    // ============================================================
    // ⭐️ 이벤트 전송 (V2 기능 - 상세 정보 포함)
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
            log.warn("[TerraformExecutor] Failed to send LOG event: {}", e.getMessage());
        }
    }

    /**
     * Terraform 전체 시퀀스가 정상 종료된 뒤 SUCCESS 이벤트 전송.
     * V2의 상세 로직 사용 + V1의 172.x.x.x 우선순위 로직 통합
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
                    try { vmCount = Math.max(1, Integer.parseInt(String.valueOf(v))); } catch (Exception ignore) {}
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

            // terraform output 파싱
            List<Map<String, Object>> vmDetails = extractVmDetails(tfOutputs);
            List<?> vmIpAddressesRaw = null;
            Object vmIpListObj = tfOutputs.get("vm_ip_addresses");
            if (vmIpListObj instanceof List<?> list) vmIpAddressesRaw = list;

            List<String> vmIpAddresses = null;
            if (vmIpAddressesRaw != null) {
                vmIpAddresses = new ArrayList<>();
                for (Object o : vmIpAddressesRaw) vmIpAddresses.add(normalizeIp(asString(o)));
            }

            String singleIp = normalizeIp(asString(tfOutputs.get("ip_address")));
            List<ProvisionResultMessage.InstanceInfo> instances = new ArrayList<>();

            for (int i = 0; i < vmCount; i++) {
                ProvisionResultMessage.InstanceInfo info = new ProvisionResultMessage.InstanceInfo();
                String name = (vmCount == 1) ? vmName : vmName + "-" + (i + 1);
                String externalId = null;
                List<String> ipCandidates = new ArrayList<>();

                // 1) vm_details 기반 정보
                Map<String, Object> detail = null;
                if (!vmDetails.isEmpty() && i < vmDetails.size()) {
                    detail = vmDetails.get(i);
                    String detailName = asString(detail.get("name"));
                    if (isNotBlank(detailName)) name = detailName;

                    externalId = asString(detail.getOrDefault("moid", detail.get("id")));

                    Object cpuVal = detail.get("cpu_cores");
                    if (cpuVal instanceof Number n) cpuCores = n.intValue();

                    Object memMbVal = detail.get("memory_mb");
                    if (memMbVal instanceof Number n) memoryGb = Math.max(1, n.intValue() / 1024);

                    Object diskGbVal = detail.get("total_disk_gb");
                    if (diskGbVal instanceof Number n) diskGb = n.intValue();

                    String defaultIp = normalizeIp(asString(detail.get("ip_address")));
                    if (defaultIp != null) ipCandidates.add(defaultIp);

                    addCandidatesFromList(ipCandidates, detail.get("guest_ip_addresses"));
                }

                // 2) vm_ip_addresses 기반 정보
                if (vmIpAddresses != null && i < vmIpAddresses.size()) {
                    String ip = normalizeIp(vmIpAddresses.get(i));
                    if (ip != null) ipCandidates.add(ip);
                }

                if (singleIp != null && vmCount == 1) ipCandidates.add(singleIp);

                // 3) STATIC IP
                String ipFromTfVars = normalizeIp(asString(tfVars.get("ipv4_address")));
                if (ipFromTfVars != null) ipCandidates.add(ipFromTfVars);

                // ⭐️ [IP 선택] 172.16 대역 우선 선택
                String chosenIp = choosePreferredIp(ipCandidates);

                // ⭐️ [NIC 리스트] 172 대역만 수집 (V2 로직에 필터링 강화)
                List<String> nicAddresses = new ArrayList<>();
                if (detail != null) {
                    Object guestIpObj = detail.get("guest_ip_addresses");
                    if (guestIpObj instanceof List<?> rawList) {
                        for (Object ipObj : rawList) {
                            String ip = normalizeIp(asString(ipObj));
                            if (ip != null && ip.matches("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b")) {
                                // 172.16 우선, 그 외 172.x, 10.x 도 수집
                                if (ip.startsWith("172.") || ip.startsWith("10.")) {
                                    nicAddresses.add(ip);
                                }
                            }
                        }
                    }
                }
                String nicAddressesStr = nicAddresses.isEmpty() ? null : String.join(",", nicAddresses);

                info.setExternalId(externalId);
                info.setIpAddress(chosenIp);
                info.setNicAddresses(nicAddressesStr);
                info.setName(name);
                info.setZoneId(zoneId);
                info.setProviderType(providerType);
                info.setCpuCores(cpuCores);
                info.setMemoryGb(memoryGb);
                info.setDiskGb(diskGb);

                log.info("[TerraformExecutor] VM index={}, name={}, externalId={}, chosenIp={}, nicAddresses={}",
                        i, name, externalId, chosenIp, nicAddressesStr);
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
        } catch (Exception ex) {
            log.warn("Failed to send ERROR event: {}", ex.getMessage());
        }
    }

    // ============================================================
    // Helper: Terraform 실행 및 Output 파싱
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
        try { executeCommand(dir, env, cmd); } catch (RuntimeException e) {
            if (failHard) throw wrapTerraformException(e);
            else log.warn("Command failed (ignored): {}", String.join(" ", cmd));
        }
    }

    private void executeCommand(File dir, Map<String, String> env, String... cmd) {
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir).redirectErrorStream(false); // redirectErrorStream false로 분리
        if (env != null) pb.environment().putAll(env);

        final String jobId = currentJobId.get();
        final String step = resolveStepName(cmd);

        try {
            Process p = pb.start();

            StringBuilder stdoutBuf = new StringBuilder();
            StringBuilder stderrBuf = new StringBuilder();
            CountDownLatch latch = new CountDownLatch(2);

            // Stdout Thread
            new Thread(() -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stdoutBuf.append(line).append('\n');
                        log.info("[terraform] {}", line);
                        sendLogEvent(jobId, step, line);
                    }
                } catch (IOException ignore) {} finally { latch.countDown(); }
            }).start();

            // Stderr Thread
            new Thread(() -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getErrorStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stderrBuf.append(line).append('\n');
                        log.error("[terraform] {}", line);
                        sendLogEvent(jobId, step, line);
                    }
                } catch (IOException ignore) {} finally { latch.countDown(); }
            }).start();

            boolean finished = p.waitFor(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) { p.destroyForcibly(); throw new RuntimeException("Command timed out"); }

            latch.await(5, TimeUnit.SECONDS);

            if (p.exitValue() != 0) {
                String msg = "Command failed: " + String.join(" ", cmd) + "\n" + mergeOutErr(stdoutBuf.toString(), stderrBuf.toString());
                throw wrapTerraformException(new RuntimeException(msg));
            }
        } catch (Exception e) { throw wrapTerraformException(new RuntimeException("Command failed to start", e)); }
    }

    private Map<String, Object> readTerraformOutputs(File dir, Map<String, String> env) {
        try {
            ProcessBuilder pb = new ProcessBuilder("terraform", "output", "-json").directory(dir);
            if (env != null) pb.environment().putAll(env);
            Process p = pb.start();

            String json = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            p.waitFor();
            if (p.exitValue() != 0 || json.isBlank()) return Collections.emptyMap();

            Map<String, TerraformOutput> raw = om.readValue(json, new TypeReference<Map<String, TerraformOutput>>() {});
            Map<String, Object> flat = new LinkedHashMap<>();
            for (Map.Entry<String, TerraformOutput> e : raw.entrySet()) {
                flat.put(e.getKey(), e.getValue() != null ? e.getValue().value : null);
            }
            return flat;
        } catch (Exception e) {
            log.warn("Failed to read terraform outputs: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    // ===== Module Staging Helpers =====
    private void stageModuleFromClasspath(File targetDir) throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        String pattern = "classpath:" + moduleResourcePath + "/**/*.tf";
        Resource[] resources = resolver.getResources(pattern);
        if (resources == null || resources.length == 0) {
            pattern = "classpath:" + moduleResourcePath + "/**/*.tf.json";
            resources = resolver.getResources(pattern);
        }
        if (resources == null || resources.length == 0) throw new IOException("No Terraform files found in classpath");
        for (Resource resource : resources) {
            String filename = resource.getFilename();
            if (filename == null) continue;
            File targetFile = new File(targetDir, filename);
            try (InputStream is = resource.getInputStream(); FileOutputStream fos = new FileOutputStream(targetFile)) {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = is.read(buffer)) != -1) fos.write(buffer, 0, bytesRead);
            }
        }
    }

    private void stageModuleFromSource(File dir, Map<String, String> env, String source) {
        executeCommand(dir, env, "terraform", "init", "-from-module=" + source, "-input=false", "-no-color");
    }

    private void stageModuleFromLocal(File dir, Map<String, String> env, Path localPath) {
        try { copyDirectoryRecursively(localPath, dir.toPath()); } catch (Exception e) { throw new RuntimeException("Failed to stage module from local", e); }
    }

    private void copyDirectoryRecursively(Path src, Path dst) throws IOException {
        if (!Files.exists(dst)) Files.createDirectories(dst);
        Files.walk(src).forEach(p -> {
            try {
                Path target = dst.resolve(src.relativize(p));
                if (Files.isDirectory(p)) { if (!Files.exists(target)) Files.createDirectories(target); }
                else { Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING); }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        });
    }

    // ===== Data Mapping =====

    private Map<String, Object> buildTfVarsFromMessage(ProvisionJobMessage msg, String vmName) {
        Map<String, Object> tf = new LinkedHashMap<>();
        tf.put("vsphere_server", nvl(System.getenv("VSPHERE_SERVER"), "vcenter.fisa.com"));
        tf.put("vsphere_user", nvl(System.getenv("VSPHERE_USER"), "administrator@fisa.ce5"));
        tf.put("vsphere_password", System.getenv("VSPHERE_PASSWORD"));
        tf.put("allow_unverified_ssl", Boolean.parseBoolean(nvl(System.getenv("VSPHERE_ALLOW_UNVERIFIED_SSL"), "true")));

        Object datacenterObj = safeInvoke(msg, "getDatacenter");
        tf.put("datacenter", nvl(datacenterObj != null ? String.valueOf(datacenterObj) : null, nvl(System.getenv("VSPHERE_DATACENTER"), "ce5-3")));

        Object clusterObj = safeInvoke(msg, "getCluster");
        tf.put("cluster", nvl(clusterObj != null ? String.valueOf(clusterObj) : null, nvl(System.getenv("VSPHERE_CLUSTER"), "")));

        Object datastoreObj = safeInvoke(msg, "getDatastore");
        tf.put("datastore", nvl(datastoreObj != null ? String.valueOf(datastoreObj) : null, nvl(System.getenv("VSPHERE_DATASTORE"), "HDD1 (1)")));

        Object folderObj = safeInvoke(msg, "getFolder");
        tf.put("folder", nvl(folderObj != null ? String.valueOf(folderObj) : null, ""));

        tf.put("vm_name", vmName);
        tf.put("vm_count", safe((Number) safeInvoke(msg, "getVmCount"), 1));

        Object addCfg = safeInvoke(msg, "getAdditionalConfig");
        Map<String, Object> add = (addCfg instanceof Map) ? (Map<String, Object>) addCfg : Collections.emptyMap();
        String templateName = asString(add.get("templateName"));
        if (!isNotBlank(templateName)) {
            Object templateObj = safeInvoke(msg, "getTemplate");
            if (templateObj != null) templateName = reflectString(templateObj, "getItemName");
        }
        tf.put("template_name", templateName);
        tf.put("cpu_cores", safe((Number) safeInvoke(msg, "getCpuCores"), 2));
        tf.put("memory_gb", safe((Number) safeInvoke(msg, "getMemoryGb"), 4));
        tf.put("disk_gb", safe((Number) safeInvoke(msg, "getDiskGb"), 50));

        String diskProv = asString(add.getOrDefault("diskProvisioning", "thin"));
        tf.put("disk_provisioning", diskProv.toLowerCase());

        Long teamId = toLong(safeInvoke(msg, "getTeamId"));
        Long zoneId = toLong(safeInvoke(msg, "getZoneId"));
        List<String> networks = resolveNetworkNames(msg, add, teamId, zoneId);
        if (!networks.isEmpty()) {
            tf.put("network", networks.get(0));
            if (networks.size() > 1) tf.put("extra_networks", networks.subList(1, networks.size()));
        } else {
            tf.put("network", nvl(System.getenv("VSPHERE_NETWORK"), "PG-WAN"));
        }

        String ipMode = asString(add.getOrDefault("ipAllocationMode", "DHCP"));
        tf.put("ip_allocation_mode", ipMode.toUpperCase());

        Object netObj = safeInvoke(msg, "getNet");
        if (netObj != null) {
            Object ipv4Obj = safeInvoke(netObj, "getIpv4");
            if (ipv4Obj != null) {
                String ipv4 = reflectString(ipv4Obj, "getAddress", "getIp");
                if (isNotBlank(ipv4)) {
                    tf.put("ipv4_address", ipv4);
                    Number netmask = (Number) safeInvoke(ipv4Obj, "getNetmask");
                    if (netmask != null) tf.put("ipv4_netmask", netmask.intValue());
                    String gateway = reflectString(ipv4Obj, "getGateway");
                    if (isNotBlank(gateway)) tf.put("ipv4_gateway", gateway);
                }
            }
            Object dnsObj = safeInvoke(netObj, "getDns");
            if (dnsObj != null && dnsObj instanceof List) {
                List<?> dnsList = (List<?>) dnsObj;
                if (!dnsList.isEmpty()) {
                    List<String> dnsServers = new ArrayList<>();
                    for (Object dns : dnsList) dnsServers.add(String.valueOf(dns));
                    tf.put("dns_servers", dnsServers);
                }
            }
        }
        tf.put("domain", "local");
        Object tagsObj = safeInvoke(msg, "getTags");
        if (tagsObj instanceof Map) tf.put("tags", tagsObj);
        else tf.put("tags", Collections.emptyMap());
        return tf;
    }

    private List<String> resolveNetworkNames(ProvisionJobMessage msg, Map<String, Object> additionalConfig, Long teamId, Long zoneId) {
        List<String> networks = new ArrayList<>();
        if (additionalConfig != null) {
            Object networksObj = additionalConfig.get("networks");
            if (networksObj instanceof List<?> list) {
                for (Object o : list) if (o != null) networks.add(String.valueOf(o));
            }
        }
        if (networks.isEmpty() && additionalConfig != null) {
            Object networkFromReq = additionalConfig.get("network");
            if (networkFromReq != null) networks.add(String.valueOf(networkFromReq));
        }
        if (networks.isEmpty()) {
            Object networkObj = safeInvoke(msg, "getNetwork");
            String base = nvl(networkObj != null ? String.valueOf(networkObj) : null, nvl(System.getenv("VSPHERE_NETWORK"), "PG-WAN"));
            if (isNotBlank(base)) networks.add(base);
        }
        // teamId 매핑 (V2 로직)
        if (teamId != null && teamId == 1L) {
            String teamVlan = "VLAN101_TeamA";
            if (!networks.contains(teamVlan)) networks.add(0, teamVlan);
        }
        if (networks.isEmpty()) networks.add("PG-WAN");
        log.info("[TerraformExecutor] Resolved networks for teamId={}, zoneId={} => {}", teamId, zoneId, networks);
        return networks;
    }

    // ===== Utils =====

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

    private void writeVarsFile(File file, Map<String, Object> vars) {
        try (FileOutputStream fos = new FileOutputStream(file); OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
            om.writeValue(osw, vars);
        } catch (IOException e) { throw new RuntimeException("Failed to write tfvars", e); }
    }

    private RuntimeException wrapTerraformException(RuntimeException e) {
        String msg = e.getMessage() == null ? "" : e.getMessage();
        StringBuilder hint = new StringBuilder();
        if (msg.contains("Invalid single-argument block definition")) hint.append("\n\n[Hint] Check Terraform syntax.");
        if (msg.toLowerCase().contains("no configuration files")) hint.append("\n\n[Hint] No .tf files found.");
        return new RuntimeException(msg + hint, e);
    }

    private String resolveStepName(String... cmd) {
        if (cmd == null || cmd.length == 0) return "terraform";
        if (cmd.length >= 2 && "terraform".equals(cmd[0])) return "terraform_" + cmd[1];
        return String.join("_", cmd);
    }

    private String mergeOutErr(String out, String err) { return out + "\n" + err; }
    private boolean hasTfFiles(File dir) { File[] list = dir.listFiles((d, name) -> name.endsWith(".tf") || name.endsWith(".tf.json")); return list != null && list.length > 0; }

    private List<Map<String, Object>> extractVmDetails(Map<String, Object> tfOutputs) {
        if (tfOutputs == null) return Collections.emptyList();
        Object detailsObj = tfOutputs.get("vm_details");
        if (!(detailsObj instanceof List<?> list)) return Collections.emptyList();
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object o : list) {
            if (o instanceof Map<?, ?> m) {
                Map<String, Object> casted = new LinkedHashMap<>();
                for (Map.Entry<?, ?> e : m.entrySet()) casted.put(String.valueOf(e.getKey()), e.getValue());
                result.add(casted);
            }
        }
        return result;
    }

    // 172.16 우선순위 IP 선택 로직 (V1 요구사항 반영)
    private String choosePreferredIp(List<String> candidates) {
        if (candidates == null || candidates.isEmpty()) return null;
        List<String> filtered = new ArrayList<>();
        for (String c : candidates) {
            String ip = normalizeIp(c);
            if (ip != null && !filtered.contains(ip)) filtered.add(ip);
        }
        if (filtered.isEmpty()) return null;

        String prefixEnv = System.getenv("INTERNAL_IP_PREFIX"); // 172.16.
        if (prefixEnv != null && !prefixEnv.isBlank()) {
            for (String ip : filtered) if (ip.startsWith(prefixEnv)) return ip;
        }

        // 172.16. 우선 확인 (하드코딩 요구사항)
        for (String ip : filtered) if (ip.startsWith("172.16.")) return ip;
        // 172. 그외
        for (String ip : filtered) if (ip.startsWith("172.")) return ip;
        // 10. 그외
        for (String ip : filtered) if (ip.startsWith("10.")) return ip;

        return filtered.get(0);
    }

    private void addCandidatesFromList(List<String> target, Object listObj) {
        if (!(listObj instanceof List<?> list)) return;
        for (Object o : list) {
            String ip = normalizeIp(asString(o));
            if (ip != null && !target.contains(ip)) target.add(ip);
        }
    }

    private String normalizeIp(String ip) {
        if (ip == null) return null;
        ip = ip.trim();
        if (ip.isEmpty() || "null".equalsIgnoreCase(ip) || "none".equalsIgnoreCase(ip)) return null;
        return ip;
    }

    // Reflection Helpers
    private Object safeInvoke(Object target, String method) { try { Method m = target.getClass().getMethod(method); return m.invoke(target); } catch (Exception ignore) { return null; } }
    private String reflectString(Object target, String... methods) { for (String m : methods) { try { Method mm = target.getClass().getMethod(m); Object v = mm.invoke(target); if (v != null) return String.valueOf(v); } catch (Exception ignore) {} } return null; }
    private String reflectFieldString(Object target, String... fields) { for (String f : fields) { try { Field field = target.getClass().getDeclaredField(f); field.setAccessible(true); Object v = field.get(target); if (v != null) return String.valueOf(v); } catch (Exception ignore) {} } return null; }
    private long parseJobId(Object v) { if (v == null) return System.currentTimeMillis(); try { if (v instanceof Number) return ((Number) v).longValue(); return Long.parseLong(String.valueOf(v)); } catch (Exception e) { return System.currentTimeMillis(); } }
    private Long toLong(Object v) { if (v == null) return null; try { if (v instanceof Number n) return n.longValue(); return Long.parseLong(String.valueOf(v)); } catch (Exception e) { return null; } }
    private String nvl(String s, String def) { return (s == null || s.isBlank()) ? def : s; }
    private boolean isNotBlank(String s) { return s != null && !s.isBlank(); }
    private int safe(Number v, int def) { return v == null ? def : v.intValue(); }
    private String asString(Object v) { return v == null ? null : String.valueOf(v); }
    private String firstLine(String s) { if (s == null) return ""; int i = s.indexOf('\n'); return i >= 0 ? s.substring(0, i) : s; }
    private String normalizeEmptyToNull(String s) { return (s == null || s.trim().isEmpty()) ? null : s.trim(); }

    private static class TerraformOutput { public Object value; }
}