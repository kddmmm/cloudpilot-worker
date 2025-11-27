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

        ProvisionJobMessage msg = null;

        try {
            msg = coerceToProvisionJobMessage(message);

            final String jobId   = msg.getJobId();
            final String corrIn  = toCorrString(correlationIdRaw) != null ? toCorrString(correlationIdRaw) : jobId;

            log.info("[Worker:{}] ===== Received Provision Job =====", workerId);
            log.info("[Worker:{}] JobID: {}, CorrelationID: {}, isFinalAttempt: {}", workerId, jobId, corrIn, isFinalAttempt);
            log.info("[Worker:{}] VM Config: name={}, count={}, cpu={}, mem={}GB, disk={}GB",
                    workerId, msg.getVmName(), msg.getVmCount(), msg.getCpuCores(), msg.getMemoryGb(), msg.getDiskGb());

            if (msg.getVmName() == null || msg.getVmName().isBlank()) {
                throw new IllegalArgumentException("vmName is required");
            }

            // ============================================================
            // 1. Terraform 실행 (VM 생성) - ✅ SUCCESS 이벤트는 여기서 자동 전송됨
            // ============================================================
            String vmId = terraformExecutor.execute(msg, isFinalAttempt);
            log.info("[Worker:{}] Terraform execution finished. VM ID: {}", workerId, vmId);

            // ============================================================
            // 2. IP 주소 추출 (Terraform Output & 필터링)
            // ============================================================
            String extractedIp = terraformExecutor.getProvisionedIp(jobId);
            log.info("[Worker:{}] Final selected IP from Terraform: {}", workerId, extractedIp);

            // ============================================================
            // 3. Ansible 실행
            // ============================================================
            String targetIp = extractedIp;

            if (targetIp != null && !targetIp.isBlank() && !"null".equalsIgnoreCase(targetIp)) {
                try {
                    log.info("[Worker:{}] Starting Ansible provisioning for IP: {}", workerId, targetIp);
                    ansibleExecutor.execute(targetIp, msg, isFinalAttempt);
                    log.info("[Worker:{}] Ansible provisioning completed successfully.", workerId);

                    // ✅ Ansible 완료 알림 (LOG 이벤트로 전송)
                    sendAnsibleCompletedEvent(jobId, targetIp);

                } catch (Exception e) {
                    log.error("[Worker:{}] ⚠️ Ansible failed, but VM created. Error: {}", workerId, e.getMessage());
                    // Ansible 실패는 무시 (VM은 이미 생성됨)
                }
            } else {
                log.warn("[Worker:{}] ⚠️ Skipping Ansible: No valid IP address found in Terraform output.", workerId);
            }

            // ✅ SUCCESS는 TerraformExecutor에서 이미 전송했으므로 여기서는 전송하지 않음
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
     * ✅ Ansible 완료 알림 (LOG 이벤트)
     * - VM 생성은 Terraform에서 이미 완료되었으므로
     * - Ansible 완료는 LOG 이벤트로만 기록
     */
    private void sendAnsibleCompletedEvent(String jobId, String targetIp) {
        try {
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.LOG);
            result.setStatus("LOG");
            result.setStep("ansible_provision");
            result.setMessage("Ansible provisioning completed for IP: " + targetIp);
            result.setTimestamp(OffsetDateTime.now());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(jobId);
                m.getMessageProperties().setHeader("jobId", jobId);
                return m;
            });

            log.info("[Worker:{}] Sent Ansible completion LOG event for jobId={}", workerId, jobId);
        } catch (Exception e) {
            log.warn("[Worker:{}] Failed to send Ansible completion event: {}", workerId, e.getMessage());
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