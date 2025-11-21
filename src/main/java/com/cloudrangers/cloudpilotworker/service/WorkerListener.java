package com.cloudrangers.cloudpilotworker.service;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.dto.ProvisionResultMessage;
import com.cloudrangers.cloudpilotworker.executor.AnsibleExecutor;
import com.cloudrangers.cloudpilotworker.executor.TerraformExecutor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class WorkerListener {

    private static final Logger log = LoggerFactory.getLogger(WorkerListener.class);

    private final RabbitTemplate rabbitTemplate;
    private final TerraformExecutor terraformExecutor;
    private final AnsibleExecutor ansibleExecutor;
    private final ObjectMapper objectMapper;

    @Value("${worker.id}") private String workerId;
    @Value("${rabbitmq.exchange.result.name}") private String resultExchange;
    @Value("${rabbitmq.routing-key.result}")   private String resultRoutingKey;

    public WorkerListener(RabbitTemplate rabbitTemplate,
                          TerraformExecutor terraformExecutor,
                          AnsibleExecutor ansibleExecutor,
                          ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.terraformExecutor = terraformExecutor;
        this.ansibleExecutor = ansibleExecutor;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(
            queues = "${rabbitmq.queue.provision.name}",
            containerFactory = "rabbitListenerContainerFactory"
    )
    public void handleProvision(Object raw,
                                @Header(name = AmqpHeaders.CORRELATION_ID, required = false) Object correlationIdRaw) {
        ProvisionJobMessage msg = null;
        try {
            msg = coerceToProvisionJobMessage(raw);

            final String jobId   = msg.getJobId();
            final String corrIn  = toCorrString(correlationIdRaw) != null ? toCorrString(correlationIdRaw) : jobId;
            final String corrOut = jobId;
            final String jobIdHeader = jobId;

            log.info("[Worker:{}] ===== Received Provision Job =====", workerId);
            log.info("[Worker:{}] JobID: {}, CorrelationID: {}", workerId, jobId, corrIn);
            log.info("[Worker:{}] VM Config: name={}, count={}, cpu={}, mem={}GB, disk={}GB",
                    workerId, msg.getVmName(), msg.getVmCount(), msg.getCpuCores(), msg.getMemoryGb(), msg.getDiskGb());

            if (msg.getVmName() == null || msg.getVmName().isBlank()) {
                throw new IllegalArgumentException("vmName is required");
            }

            // ============================================================
            // 1. Terraform 실행 (VM 생성)
            // ============================================================
            String vmId = terraformExecutor.execute(msg);
            log.info("[Worker:{}] Terraform execution finished. VM ID: {}", workerId, vmId);

            // ============================================================
            // 2. IP 주소 추출 (Terraform Output & 필터링)
            // ============================================================
            String extractedIp = terraformExecutor.getProvisionedIp(jobId);
            log.info("[Worker:{}] Final selected IP from Terraform: {}", workerId, extractedIp);

            // ============================================================
            // 3. Ansible 실행 (수정됨: BackendClient 제거, Terraform IP 사용)
            // ============================================================
            String targetIp = extractedIp; // ⭐️ 무조건 Terraform에서 필터링한 172. 대역 IP 사용

            if (targetIp != null && !targetIp.isBlank() && !"null".equalsIgnoreCase(targetIp)) {
                try {
                    // Ansible 실행 (172 IP 사용)
                    log.info("[Worker:{}] Starting Ansible provisioning for IP: {}", workerId, targetIp);
                    ansibleExecutor.execute(targetIp, msg);
                    log.info("[Worker:{}] Ansible provisioning completed successfully.", workerId);

                } catch (Exception e) {
                    log.error("[Worker:{}] ⚠️ Ansible failed, but VM created. Error: {}", workerId, e.getMessage());
                }
            } else {
                log.warn("[Worker:{}] ⚠️ Skipping Ansible: No valid IP address found in Terraform output.", workerId);
            }

            // ============================================================
            // 4. 결과 전송 (SUCCESS)
            // ============================================================
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.SUCCESS);
            result.setStatus("SUCCEEDED");
            result.setVmId(vmId);
            result.setMessage("VM created and configured successfully");
            result.setTimestamp(OffsetDateTime.now());

            List<ProvisionResultMessage.InstanceInfo> instances = new ArrayList<>();
            int vmCount = (msg.getVmCount() != null && msg.getVmCount() > 0) ? msg.getVmCount() : 1;

            for (int i = 0; i < vmCount; i++) {
                ProvisionResultMessage.InstanceInfo inst = new ProvisionResultMessage.InstanceInfo();

                String baseName = msg.getVmName();
                String name = (vmCount == 1) ? baseName : baseName + "-" + (i + 1);
                inst.setName(name);
                inst.setExternalId((vmCount == 1) ? vmId : vmId + "#" + (i + 1));

                if (msg.getZoneId() != null) inst.setZoneId(msg.getZoneId());
                if (msg.getProviderType() != null) inst.setProviderType(String.valueOf(msg.getProviderType()));
                inst.setCpuCores(msg.getCpuCores());
                inst.setMemoryGb(msg.getMemoryGb());
                inst.setDiskGb(msg.getDiskGb());

                // 최종 IP 설정
                inst.setIpAddress(targetIp);

                if (msg.getOs() != null && msg.getOs().getFamily() != null) {
                    inst.setOsType(msg.getOs().getFamily());
                } else if (msg.getTemplate() != null && msg.getTemplate().getGuestId() != null) {
                    inst.setOsType(msg.getTemplate().getGuestId());
                }

                instances.add(inst);
            }
            result.setInstances(instances);

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(corrOut);
                m.getMessageProperties().setHeader("jobId", jobIdHeader);
                return m;
            });

            log.info("[Worker:{}] ✓ Job completed. jobId={}, IP={}", workerId, jobId, targetIp);

        } catch (Exception e) {
            // 에러 처리 로직
            final String fallbackCorr =
                    (msg != null && msg.getJobId() != null && !msg.getJobId().isBlank())
                            ? msg.getJobId()
                            : (toCorrString(correlationIdRaw) != null ? toCorrString(correlationIdRaw) : "unknown");

            log.error("[Worker:{}] ✗ Job failed. jobId={}, error={}", workerId, fallbackCorr, e.getMessage());

            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(fallbackCorr);
            result.setEventType(ProvisionResultMessage.EventType.ERROR);
            result.setStatus("FAILED");
            result.setMessage(e.getMessage());
            result.setTimestamp(OffsetDateTime.now());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(fallbackCorr);
                return m;
            });

            throw new AmqpRejectAndDontRequeueException("Listener failed", e);
        }
    }

    private ProvisionJobMessage coerceToProvisionJobMessage(Object raw) throws Exception {
        if (raw instanceof ProvisionJobMessage pjm) return pjm;
        if (raw instanceof Message amqp) {
            byte[] body = amqp.getBody();
            try {
                return objectMapper.readValue(body, ProvisionJobMessage.class);
            } catch (Exception e) { }
            try {
                Map<String, Object> m = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                return objectMapper.convertValue(m, ProvisionJobMessage.class);
            } catch (Exception e) {
                throw new IllegalArgumentException("Unsupported payload", e);
            }
        }
        if (raw instanceof String s) {
            return objectMapper.readValue(s, ProvisionJobMessage.class);
        }
        if (raw instanceof byte[] b) {
            return objectMapper.readValue(b, ProvisionJobMessage.class);
        }
        if (raw instanceof Map<?, ?> m) {
            return objectMapper.convertValue(m, ProvisionJobMessage.class);
        }
        throw new IllegalArgumentException("Unsupported payload type: " + raw.getClass().getName());
    }

    private String toCorrString(Object raw) {
        if (raw == null) return null;
        return String.valueOf(raw);
    }
}