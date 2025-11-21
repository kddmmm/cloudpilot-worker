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

    // ===== 외부에서 호출하는 진입점 =====

    public String execute(ProvisionJobMessage msg) {
        Objects.requireNonNull(msg, "ProvisionJobMessage must not be null");

        Object rawJobId = safeInvoke(msg, "getJobId");
        String jobIdStr = rawJobId != null ? String.valueOf(rawJobId) : String.valueOf(System.currentTimeMillis());
        long jobId = parseJobId(rawJobId);

        currentJobId.set(jobIdStr);
        try {
            String vmName = nvl(msg.getVmName(), "vm-" + jobId);
            String action = resolveAction(msg);

            File workDir = new File("/tmp/terraform/" + jobIdStr);
            if (!workDir.exists() && !workDir.mkdirs()) {
                throw new RuntimeException("Failed to create workDir: " + workDir.getAbsolutePath());
            }

            Map<String, Object> tfVars = buildTfVarsFromMessage(msg, vmName);
            Map<String, Object> tfOutputs = Collections.emptyMap();

            try {
                // 실제 Terraform 실행
                execute(jobId, vmName, action, workDir, tfVars);

                // 프로비저닝(apply)일 때만 SUCCESS 이벤트 전송
                if ("apply".equalsIgnoreCase(action)) {
                    Map<String, String> env = new HashMap<>();
                    env.put("TF_IN_AUTOMATION", "1");
                    tfOutputs = readTerraformOutputs(workDir, env);
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

    // ===== LOG / SUCCESS / ERROR 이벤트 전송 =====

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
     * Terraform 전체 시퀀스가 정상 종료된 뒤, JobResultConsumer → vmProvisionService에서
     * vm_instance를 만들 수 있도록 SUCCESS 이벤트 전송.
     *
     * @param tfOutputs terraform output -json 결과의 value 맵 (ip_address, vm_ip_addresses, vm_details 등)
     */
    private void sendSuccessEvent(String jobId,
                                  ProvisionJobMessage msg,
                                  String vmName,
                                  Map<String, Object> tfVars,
                                  Map<String, Object> tfOutputs) {
        if (jobId == null) return;

        if (tfOutputs == null) {
            tfOutputs = Collections.emptyMap();
        }

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

                // IP 후보
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
                // 최종 Public IP 선택
                // ===========================
                String chosenIp = choosePreferredIp(ipCandidates);

                // ===========================
                // ⭐ NIC 전체 리스트 생성 (IPv4 전용, 172 대역만)
                // ===========================
                List<String> nicAddresses = new ArrayList<>();

                if (detail != null) {
                    Object guestIpObj = detail.get("guest_ip_addresses");
                    if (guestIpObj instanceof List<?> rawList) {
                        for (Object ipObj : rawList) {
                            String ip = normalizeIp(asString(ipObj));
                            if (ip != null
                                    && ip.matches("\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b")
                                    && ip.startsWith("172.")) {   // 172 대역만 수집
                                nicAddresses.add(ip);
                            }
                        }
                    }
                }

                // 리스트를 콤마 구분 문자열로 변환
                String nicAddressesStr = nicAddresses.isEmpty()
                        ? null
                        : String.join(",", nicAddresses);

                // ===========================
                // info 필드 설정
                // ===========================
                info.setExternalId(externalId);
                info.setIpAddress(chosenIp);
                info.setNicAddresses(nicAddressesStr);

                info.setName(name);
                info.setZoneId(zoneId);
                info.setProviderType(providerType);
                info.setCpuCores(cpuCores);
                info.setMemoryGb(memoryGb);
                info.setDiskGb(diskGb);
                info.setOsType(null);

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

    private String resolveStepName(String... cmd) {
        if (cmd == null || cmd.length == 0) return "terraform";
        if (cmd.length >= 2 && "terraform".equals(cmd[0])) {
            return "terraform_" + cmd[1];
        }
        return String.join("_", cmd);
    }

    // ===== 모듈 스테이징 =====

    private void ensureModulePresent(File dir, Map<String, String> env) {
        log.debug("=== ensureModulePresent START ===");
        log.debug("Working directory: {}", dir.getAbsolutePath());
        log.debug("useEmbeddedModule: {}", useEmbeddedModule);
        log.debug("moduleResourcePath: '{}'", moduleResourcePath);

        if (hasTfFiles(dir)) {
            log.info("✓ Terraform config already present in {}", dir.getAbsolutePath());
            return;
        }

        log.info("No .tf files found. Starting module staging...");

        if (useEmbeddedModule && isNotBlank(moduleResourcePath)) {
            log.info("✓ Using embedded module from classpath: {}", moduleResourcePath);
            try {
                stageModuleFromClasspath(dir);
                executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
                log.info("✓ Embedded module staged successfully");
                return;
            } catch (Exception e) {
                log.error("Failed to stage embedded module", e);
                throw new RuntimeException("Failed to stage embedded module from classpath: " + moduleResourcePath, e);
            }
        }

        String src = normalizeEmptyToNull(moduleSourceProp);
        if (src == null) {
            src = normalizeEmptyToNull(System.getenv("TERRAFORM_MODULE_SOURCE"));
        }

        log.debug("Resolved module source: '{}'", src);

        if (src != null) {
            log.info("✓ Using module source: {}", src);
            stageModuleFromSource(dir, env, src);
            return;
        }

        String localDir = normalizeEmptyToNull(moduleLocalDir);
        log.debug("Checking local module directory: '{}'", localDir);

        if (localDir != null) {
            Path localPath = Path.of(localDir);
            if (Files.exists(localPath) && Files.isDirectory(localPath)) {
                log.info("✓ Using local module directory: {}", localDir);
                stageModuleFromLocal(dir, env, localPath);
                return;
            } else {
                log.warn("✗ Local module directory not found or not a directory: {}", localDir);
            }
        }

        log.error("✗ No Terraform module source configured!");
        throw new RuntimeException("No Terraform configuration found and no module source configured.");
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

        log.info("✓ Extracted {} Terraform files", resources.length);

        if (!hasTfFiles(targetDir)) {
            throw new IOException("Extraction completed but no .tf files found in " + targetDir);
        }
    }

    private void stageModuleFromSource(File dir, Map<String, String> env, String source) {
        log.info("Staging module via 'terraform init -from-module={}'", source);

        try {
            executeCommand(dir, env, "terraform", "init", "-from-module=" + source, "-input=false", "-no-color");
            log.info("✓ Module staged from source");
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

    private String normalizeEmptyToNull(String s) {
        if (s == null || s.trim().isEmpty()) return null;
        return s.trim();
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

    // ===== 실행 시퀀스 + 커맨드 =====

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

            Thread tOut = new Thread(() -> {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stdoutBuf.append(line).append('\n');
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

    private void writeVarsFile(File file, Map<String, Object> vars) {
        try (FileOutputStream fos = new FileOutputStream(file);
             OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8)) {
            om.writeValue(osw, vars);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write tfvars file: " + file.getAbsolutePath(), e);
        }
    }

    // ===== 메시지 → tfvars 매핑 =====

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

        // === 네트워크 설정 (팀 VLAN 한 개만) ===
        Long teamId = toLong(safeInvoke(msg, "getTeamId"));
        Long zoneId = toLong(safeInvoke(msg, "getZoneId"));

        List<String> networks = resolveNetworkNames(msg, add, teamId, zoneId);
        if (!networks.isEmpty()) {
            tf.put("network", networks.get(0));
        } else {
            String defaultNet = normalizeEmptyToNull(System.getenv("VSPHERE_NETWORK"));
            if (defaultNet == null) {
                throw new RuntimeException("No vSphere network resolved. Please set VSPHERE_NETWORK or provide additionalConfig.network(s).");
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
     *  4) VSPHERE_NETWORK 환경변수
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
        if (networks.isEmpty() && teamId != null) {
            if (teamId == 1L) {
                // 🔥 teamId = 1 이면 VLAN101_TeamA 사용
                networks.add("VLAN101_TeamA");
                log.info("[TerraformExecutor] teamId={} → network='VLAN101_TeamA'", teamId);
            }
            // 나중에 팀 늘어나면 여기서 else-if / switch 로 추가
            // else if (teamId == 2L) { networks.add("VLAN102_TeamB"); } 등등
        }

        // 5) 글로벌 기본값: VSPHERE_NETWORK
        if (networks.isEmpty()) {
            String fromEnv = normalizeEmptyToNull(System.getenv("VSPHERE_NETWORK"));
            if (fromEnv != null) {
                networks.add(fromEnv);
                log.info("[TerraformExecutor] Using default vSphere network from env VSPHERE_NETWORK={}", fromEnv);
            }
        }

        if (networks.isEmpty()) {
            throw new RuntimeException(
                    "네트워크를 결정할 수 없습니다. teamId=" + teamId
                            + " 에 대한 매핑 또는 VSPHERE_NETWORK, additionalConfig.network(s)를 설정하세요."
            );
        }

        log.info("[TerraformExecutor] Resolved networks for teamId={}, zoneId={} => {}",
                teamId, zoneId, networks);

        return networks;
    }



    // ===== action 유추 =====

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

    // ===== 에러 힌트 =====

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

    // ===== terraform output -json 파싱 =====

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

    // ===== 리플렉션/유틸 =====

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

    private void putIfAbsent(Map<String, Object> m, String k, Object v) {
        if (!m.containsKey(k)) m.put(k, v);
    }

    private String firstLine(String s) {
        if (s == null) return "";
        int i = s.indexOf('\n');
        return i >= 0 ? s.substring(0, i) : s;
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

    // ===== IP 선택 유틸 =====

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
     * - 환경변수 INTERNAL_IP_PREFIX 가 있으면 그걸 우선 사용 (예: "172.")
     * - 없으면 172.* -> 10.* -> 그 외 순으로 선택
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

        String prefixEnv = System.getenv("INTERNAL_IP_PREFIX");
        if (prefixEnv != null && !prefixEnv.isBlank()) {
            for (String ip : filtered) {
                if (ip.startsWith(prefixEnv)) return ip;
            }
        }

        // 기본 선호: 172 / 10 대역 → 내부망으로 많이 쓰이니까
        for (String ip : filtered) {
            if (ip.startsWith("172.")) return ip;
        }
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
}
