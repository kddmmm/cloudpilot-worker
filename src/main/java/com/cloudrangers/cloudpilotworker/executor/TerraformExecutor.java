package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.dto.ProvisionResultMessage;
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

            // String ID로 경로 생성
            File workDir = new File("/tmp/terraform/" + jobIdStr);
            if (!workDir.exists() && !workDir.mkdirs()) {
                throw new RuntimeException("Failed to create workDir: " + workDir.getAbsolutePath());
            }

            Map<String, Object> tfVars = buildTfVarsFromMessage(msg, vmName);

            try {
                // 실제 Terraform 실행
                execute(jobId, vmName, action, workDir, tfVars);

                // 프로비저닝(apply)일 때만 SUCCESS 이벤트 전송
                if ("apply".equalsIgnoreCase(action)) {
                    sendSuccessEvent(jobIdStr, msg, vmName, tfVars);
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

        // ⭐️ 모듈 준비 (여기서 Output 파일 자동 생성됨)
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
    // ⭐️ [IP 추출] 172.16. 대역 필터링 로직 포함
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
                if (ip.startsWith("172.16.")) return ip; // 172면 바로 리턴
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

            // 4. 최후의 수단: 그냥 잡히는 IP라도 반환 (Ansible 실패 가능성 있음)
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

    // ===== 모듈 스테이징 및 자동 Output 생성 =====

    private void ensureModulePresent(File dir, Map<String, String> env) {
        log.debug("=== ensureModulePresent START ===");
        log.debug("Working directory: {}", dir.getAbsolutePath());

        if (hasTfFiles(dir)) {
            log.info("✓ Terraform config already present in {}", dir.getAbsolutePath());
            // 파일이 이미 있어도 Output 파일은 무조건 재생성 (안전장치)
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

        // ⭐️ [핵심] 모듈 복사 완료 후, Worker 전용 Output 파일 생성
        generateExtraOutputsFile(dir);

        // Init 실행
        executeCommand(dir, env, "terraform", "init", "-input=false", "-no-color");
    }

    // ⭐️ [추가된 메서드] Worker 전용 Output 파일 생성 (guest_ip_addresses 포함)
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

    // ===== Helper Methods =====

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

    private void sendSuccessEvent(String jobId, ProvisionJobMessage msg, String vmName, Map<String, Object> tfVars) {
        if (jobId == null) return;
        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.SUCCESS);
            result.setStatus("SUCCEEDED");
            result.setStep("terraform_apply");
            result.setTimestamp(OffsetDateTime.now());
            result.setMessage("Terraform apply succeeded");
            result.setVmId(vmName);
            result.setInstances(new ArrayList<>());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                return m;
            });
        } catch (Exception e) {
            log.warn("[TerraformExecutor] Failed to send SUCCESS event: {}", e.getMessage());
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

    private String resolveStepName(String... cmd) {
        if (cmd == null || cmd.length == 0) return "terraform";
        if (cmd.length >= 2 && "terraform".equals(cmd[0])) {
            return "terraform_" + cmd[1];
        }
        return String.join("_", cmd);
    }

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
            hint.append("\n\n[Hint] Check Terraform syntax.");
        }
        return new RuntimeException(msg + hint, e);
    }

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
        try {
            copyDirectoryRecursively(localPath, dir.toPath());
        } catch (Exception e) { throw new RuntimeException("Failed to stage module from local", e); }
    }

    private void copyDirectoryRecursively(Path src, Path dst) throws IOException {
        if (!Files.exists(dst)) Files.createDirectories(dst);
        Files.walk(src).forEach(p -> {
            try {
                Path target = dst.resolve(src.relativize(p));
                if (Files.isDirectory(p)) {
                    if (!Files.exists(target)) Files.createDirectories(target);
                } else {
                    Files.copy(p, target, StandardCopyOption.REPLACE_EXISTING);
                }
            } catch (IOException e) { throw new UncheckedIOException(e); }
        });
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
        try { executeCommand(dir, env, cmd); } catch (RuntimeException e) {
            if (failHard) throw wrapTerraformException(e);
            else log.warn("Command failed (ignored): {}", String.join(" ", cmd));
        }
    }

    private void executeCommand(File dir, Map<String, String> env, String... cmd) {
        ProcessBuilder pb = new ProcessBuilder(cmd).directory(dir).redirectErrorStream(true);
        if (env != null) pb.environment().putAll(env);
        try {
            Process p = pb.start();

            // 로그 출력 스레드
            new Thread(() -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        log.info("[terraform] {}", line);
                        sendLogEvent(currentJobId.get(), resolveStepName(cmd), line);
                    }
                } catch (IOException ignore) {}
            }).start();

            boolean finished = p.waitFor(DEFAULT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (!finished) { p.destroyForcibly(); throw new RuntimeException("Command timed out"); }
            if (p.exitValue() != 0) { throw wrapTerraformException(new RuntimeException("Command failed: " + String.join(" ", cmd))); }
        } catch (Exception e) { throw wrapTerraformException(new RuntimeException("Command failed to start", e)); }
    }

    private void writeVarsFile(File file, Map<String, Object> vars) {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            om.writeValue(fos, vars);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write tfvars", e);
        }
    }

    private Map<String, Object> buildTfVarsFromMessage(ProvisionJobMessage msg, String vmName) {
        // (기존과 동일 - 생략 없이 포함)
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
        if (teamId != null && teamId == 1L) {
            String teamVlan = "VLAN101_TeamA";
            String wanNet = "PG-WAN";
            if (!networks.contains(teamVlan)) networks.add(0, teamVlan);
            if (!networks.contains(wanNet)) networks.add(wanNet);
        }
        if (networks.isEmpty()) networks.add("PG-WAN");
        log.info("[TerraformExecutor] Resolved networks for teamId={}, zoneId={} => {}", teamId, zoneId, networks);
        return networks;
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
    private void putIfAbsent(Map<String, Object> m, String k, Object v) { if (!m.containsKey(k)) m.put(k, v); }
    private String firstLine(String s) { if (s == null) return ""; int i = s.indexOf('\n'); return i >= 0 ? s.substring(0, i) : s; }
    private String mergeOutErr(String out, String err) { return out + "\n" + err; }
    private String normalizeEmptyToNull(String s) { return (s == null || s.trim().isEmpty()) ? null : s.trim(); }
    private boolean hasTfFiles(File dir) {
        File[] list = dir.listFiles((d, name) -> name.endsWith(".tf") || name.endsWith(".tf.json"));
        return list != null && list.length > 0;
    }
}