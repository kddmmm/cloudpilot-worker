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
    @Value("${spring.rabbitmq.listener.simple.retry.max-attempts:3}") private int maxAttempts;

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
    public void handleProvision(Message message,
                                @Header(name = AmqpHeaders.CORRELATION_ID, required = false) Object correlationIdRaw) {

        // ⭐️ 현재 시도 횟수 확인 (x-death 헤더에서)
        boolean isFinalAttempt = isFinalAttempt(message);

        log.info("[Worker:{}] Message received. isFinalAttempt={}", workerId, isFinalAttempt);

        // ✅ message 전체를 넘김 (byte[] 추출 삭제)
        ProvisionJobMessage msg = null;

        try {
            msg = coerceToProvisionJobMessage(message);  // ✅ message 전체 전달

            final String jobId   = msg.getJobId();
            final String corrIn  = toCorrString(correlationIdRaw) != null ? toCorrString(correlationIdRaw) : jobId;
            final String corrOut = jobId;
            final String jobIdHeader = jobId;

            log.info("[Worker:{}] ===== Received Provision Job =====", workerId);
            log.info("[Worker:{}] JobID: {}, CorrelationID: {}, isFinalAttempt: {}", workerId, jobId, corrIn, isFinalAttempt);
            log.info("[Worker:{}] VM Config: name={}, count={}, cpu={}, mem={}GB, disk={}GB",
                    workerId, msg.getVmName(), msg.getVmCount(), msg.getCpuCores(), msg.getMemoryGb(), msg.getDiskGb());

            if (msg.getVmName() == null || msg.getVmName().isBlank()) {
                throw new IllegalArgumentException("vmName is required");
            }

            // ============================================================
            // 1. Terraform 실행 (VM 생성) - ⭐️ isFinalAttempt 전달
            // ============================================================
            String vmId = terraformExecutor.execute(msg, isFinalAttempt);
            log.info("[Worker:{}] Terraform execution finished. VM ID: {}", workerId, vmId);

            // ============================================================
            // 2. IP 주소 추출 (Terraform Output & 필터링)
            // ============================================================
            String extractedIp = terraformExecutor.getProvisionedIp(jobId);
            log.info("[Worker:{}] Final selected IP from Terraform: {}", workerId, extractedIp);

            // ============================================================
            // 3. Ansible 실행 - ⭐️ isFinalAttempt 전달
            // ============================================================
            String targetIp = extractedIp;

            if (targetIp != null && !targetIp.isBlank() && !"null".equalsIgnoreCase(targetIp)) {
                try {
                    log.info("[Worker:{}] Starting Ansible provisioning for IP: {}", workerId, targetIp);
                    ansibleExecutor.execute(targetIp, msg, isFinalAttempt);
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

    /**
     * ⭐️ 마지막 시도인지 확인
     * RabbitMQ 재시도 메커니즘에서 현재 시도 횟수를 확인
     */
    private boolean isFinalAttempt(Message message) {
        try {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> xDeathHeader =
                    (List<Map<String, Object>>) message.getMessageProperties().getHeaders().get("x-death");

            // 1) DLX/Retry 큐를 쓰는 경우: x-death 기반으로 최종 시도 판단
            if (xDeathHeader != null && !xDeathHeader.isEmpty()) {
                Map<String, Object> death = xDeathHeader.get(0);
                Object countObj = death.get("count");

                long count;
                if (countObj instanceof Number n) {
                    count = n.longValue();
                } else {
                    count = Long.parseLong(String.valueOf(countObj));
                }

                boolean isFinal = count >= (maxAttempts - 1);
                log.debug("x-death count: {}, maxAttempts: {}, isFinal: {}", count, maxAttempts, isFinal);
                return isFinal;
            }

            // 2) x-death가 없는 경우
            //    → 지금 구조에선 재시도/재전송이 없으므로,
            //       이 시도가 곧 "첫 시도이자 마지막 시도"라고 판단
            log.debug("No x-death header found - treating as FINAL attempt (no retry queue configured)");
            return true;

        } catch (Exception e) {
            // 혹시 에러가 나도, 로그는 남는 게 낫다 → 보수적으로 true
            log.warn("Failed to check if final attempt: {}", e.getMessage());
            return true;
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