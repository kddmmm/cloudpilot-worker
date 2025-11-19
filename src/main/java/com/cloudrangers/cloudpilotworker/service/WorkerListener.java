package com.cloudrangers.cloudpilotworker.service;

import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.cloudrangers.cloudpilotworker.dto.ProvisionResultMessage;
import com.cloudrangers.cloudpilotworker.executor.TerraformExecutor;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
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

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class WorkerListener {

    private static final Logger log = LoggerFactory.getLogger(WorkerListener.class);

    private final RabbitTemplate rabbitTemplate;
    private final TerraformExecutor terraformExecutor;
    private final ObjectMapper objectMapper;

    @Value("${worker.id}") private String workerId;
    @Value("${rabbitmq.exchange.result.name}") private String resultExchange;
    @Value("${rabbitmq.routing-key.result}")   private String resultRoutingKey;

    public WorkerListener(RabbitTemplate rabbitTemplate,
                          TerraformExecutor terraformExecutor,
                          ObjectMapper objectMapper) {
        this.rabbitTemplate = rabbitTemplate;
        this.terraformExecutor = terraformExecutor;
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
            log.info("[Worker:{}] Provider: type={}, zoneId={}, template(item={}, moid={}), netMode={}",
                    workerId,
                    msg.getProviderType(),
                    msg.getZoneId(),
                    msg.getTemplate() != null ? msg.getTemplate().getItemName() : null,
                    msg.getTemplate() != null ? msg.getTemplate().getTemplateMoid() : null,
                    msg.getNet() != null ? msg.getNet().getMode() : "DHCP");

            if (msg.getTags() != null && !msg.getTags().isEmpty()) {
                log.info("[Worker:{}] Tags: {}", workerId, msg.getTags());
            }

            if (raw instanceof Message m) {
                log.debug("[Worker:{}] Message details - contentType={}, __TypeId__={}",
                        workerId,
                        m.getMessageProperties().getContentType(),
                        m.getMessageProperties().getHeaders().get("__TypeId__"));
            }

            log.info("[Worker:{}] ===================================", workerId);

            // 기본 검증
            if (msg.getVmName() == null || msg.getVmName().isBlank()) {
                throw new IllegalArgumentException("vmName is required");
            }
            if (msg.getCpuCores() == null || msg.getCpuCores() <= 0) {
                throw new IllegalArgumentException("cpuCores must be positive");
            }
            if (msg.getMemoryGb() == null || msg.getMemoryGb() <= 0) {
                throw new IllegalArgumentException("memoryGb must be positive");
            }
            if (msg.getDiskGb() == null || msg.getDiskGb() <= 0) {
                throw new IllegalArgumentException("diskGb must be positive");
            }

            // Terraform 실행 (내부에서 LOG 이벤트는 이미 스트리밍 중)
            String vmId = terraformExecutor.execute(msg);

            // ===== SUCCESS 이벤트 + InstanceInfo 세팅 =====
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setEventType(ProvisionResultMessage.EventType.SUCCESS);
            result.setStatus("SUCCEEDED");
            result.setVmId(vmId);
            result.setMessage("ok");
            result.setTimestamp(OffsetDateTime.now());

            // TODO: 지금은 요청 스펙 기반으로만 InstanceInfo 채움.
            //       나중에 terraform output -json 파싱해서 IP / moid / uuid 반영하자.
            List<ProvisionResultMessage.InstanceInfo> instances = new ArrayList<>();

            int vmCount = (msg.getVmCount() != null && msg.getVmCount() > 0) ? msg.getVmCount() : 1;
            for (int i = 0; i < vmCount; i++) {
                ProvisionResultMessage.InstanceInfo inst = new ProvisionResultMessage.InstanceInfo();

                // 이름: 단건이면 그대로, 다건이면 suffix 붙이기
                String baseName = msg.getVmName();
                String name = (vmCount == 1) ? baseName : baseName + "-" + (i + 1);
                inst.setName(name);

                // externalId: 지금은 vmId + 인덱스 정도만 전달 (추후 moid/uuid로 교체 예정)
                String externalId = (vmCount == 1) ? vmId : vmId + "#" + (i + 1);
                inst.setExternalId(externalId);

                if (msg.getZoneId() != null) {
                    inst.setZoneId(msg.getZoneId());
                }
                if (msg.getProviderType() != null) {
                    inst.setProviderType(String.valueOf(msg.getProviderType()));
                }

                inst.setCpuCores(msg.getCpuCores());
                inst.setMemoryGb(msg.getMemoryGb());
                inst.setDiskGb(msg.getDiskGb());

                // IP / OS 타입은 일단 비워두거나, 가능한 정보가 있으면 채우기
                inst.setIpAddress(null); // TODO: terraform output에서 주입
                if (msg.getOs() != null && msg.getOs().getFamily() != null) {
                    inst.setOsType(msg.getOs().getFamily());
                } else if (msg.getTemplate() != null && msg.getTemplate().getGuestId() != null) {
                    inst.setOsType(msg.getTemplate().getGuestId());
                }

                instances.add(inst);
            }

            result.setInstances(instances);

            // 결과 메시지 전송
            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(corrOut);
                m.getMessageProperties().setHeader("jobId", jobIdHeader);
                return m;
            });

            log.info("[Worker:{}] ✓ Job completed successfully. jobId={}, vmId={}, instances={}",
                    workerId, jobId, vmId, instances);

        } catch (Exception e) {
            final String fallbackCorr =
                    (msg != null && msg.getJobId() != null && !msg.getJobId().isBlank())
                            ? msg.getJobId()
                            : (toCorrString(correlationIdRaw) != null ? toCorrString(correlationIdRaw) : "unknown");
            final String jobIdHeader =
                    (msg != null && msg.getJobId() != null && !msg.getJobId().isBlank())
                            ? msg.getJobId() : null;

            log.error("[Worker:{}] ✗ Job failed. jobId/corr={}, error={}", workerId, fallbackCorr, e.getMessage());
            log.error("[Worker:{}] Stack trace:", workerId, e);

            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(fallbackCorr);
            result.setEventType(ProvisionResultMessage.EventType.ERROR);
            result.setStatus("FAILED");
            result.setMessage(e.getMessage());
            result.setTimestamp(OffsetDateTime.now());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(fallbackCorr);
                if (jobIdHeader != null) {
                    m.getMessageProperties().setHeader("jobId", jobIdHeader);
                }
                return m;
            });

            // 재큐 없이 DLX 로
            throw new AmqpRejectAndDontRequeueException("Listener failed, send to DLX", e);
        }
    }

    // ===== Payload → ProvisionJobMessage 변환 =====

    private ProvisionJobMessage coerceToProvisionJobMessage(Object raw) throws Exception {
        log.debug("[Worker:{}] ===== coerceToProvisionJobMessage START =====", workerId);
        log.debug("[Worker:{}] Input type: {}", workerId, raw.getClass().getName());

        if (raw instanceof ProvisionJobMessage pjm) {
            return pjm;
        }

        if (raw instanceof Message amqp) {
            byte[] body = amqp.getBody();
            Map<String, Object> headers = amqp.getMessageProperties().getHeaders();
            String typeId = headers != null && headers.get("__TypeId__") != null
                    ? String.valueOf(headers.get("__TypeId__")) : null;

            String bodyStr = new String(body, StandardCharsets.UTF_8);

            log.info("[Worker:{}] ===== RAW JSON =====", workerId);
            log.info("[Worker:{}] {}", workerId, bodyStr);
            log.info("[Worker:{}] ===================", workerId);

            try {
                ProvisionJobMessage result = objectMapper.readValue(body, ProvisionJobMessage.class);
                return result;
            } catch (MismatchedInputException e) {
                log.warn("[Worker:{}] Direct DTO parse failed (MismatchedInputException): {}", workerId, e.getMessage());
            } catch (Exception e) {
                log.warn("[Worker:{}] Direct DTO parse failed: {}", workerId, e.getMessage());
            }

            String s = bodyStr.trim();
            if ("java.lang.String".equals(typeId) || (s.startsWith("\"") && s.endsWith("\""))) {
                try {
                    String inner = objectMapper.readValue(s, String.class);
                    return objectMapper.readValue(inner, ProvisionJobMessage.class);
                } catch (Exception ignore) { }
            }

            try {
                Map<String, Object> m = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                return objectMapper.convertValue(m, ProvisionJobMessage.class);
            } catch (Exception e3) {
                log.error("[Worker:{}] All parsing attempts failed.", workerId, e3);
                throw new IllegalArgumentException("Unsupported payload; failed to parse JSON", e3);
            }
        }

        if (raw instanceof String s) {
            if (s.startsWith("\"") && s.endsWith("\"")) {
                String inner = objectMapper.readValue(s, String.class);
                return objectMapper.readValue(inner, ProvisionJobMessage.class);
            }
            return objectMapper.readValue(s, ProvisionJobMessage.class);
        }

        if (raw instanceof byte[] b) {
            String content = new String(b, StandardCharsets.UTF_8);
            log.info("[Worker:{}] Byte array content: {}", workerId, content);
            return objectMapper.readValue(b, ProvisionJobMessage.class);
        }

        if (raw instanceof Map<?, ?> m) {
            return objectMapper.convertValue(m, ProvisionJobMessage.class);
        }

        String preview = String.valueOf(raw);
        if (preview.length() > 200) preview = preview.substring(0, 200) + "...";
        throw new IllegalArgumentException("Unsupported payload type: " + raw.getClass().getName()
                + ", preview=" + preview);
    }

    private String toCorrString(Object raw) {
        if (raw == null) return null;
        if (raw instanceof String s) return s;
        if (raw instanceof byte[] b) return new String(b, StandardCharsets.UTF_8);
        return String.valueOf(raw);
    }
}
