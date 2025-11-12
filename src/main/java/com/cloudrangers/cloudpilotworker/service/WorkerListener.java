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

            // 람다에서 사용할 final 값
            final String jobId   = msg.getJobId();
            final String corrIn  = toCorrString(correlationIdRaw) != null ? toCorrString(correlationIdRaw) : jobId;
            final String corrOut = jobId;     // 결과 correlationId는 항상 jobId
            final String jobIdHeader = jobId; // fallback 헤더

            // 상세한 메시지 로깅
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

            // 필수 필드 검증
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

            // 실제 실행
            String vmId = terraformExecutor.execute(msg);

            // 성공 결과 통지
            ProvisionResultMessage result = new ProvisionResultMessage();
            result.setJobId(jobId);
            result.setStatus("SUCCEEDED");
            result.setVmId(vmId);
            result.setMessage("ok");

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(corrOut); // String 미지원이면 byte[]로 교체
                // m.getMessageProperties().setCorrelationId(corrOut.getBytes(StandardCharsets.UTF_8));
                m.getMessageProperties().setHeader("jobId", jobIdHeader);
                return m;
            });

            log.info("[Worker:{}] ✓ Job completed successfully. jobId={}, vmId={}", workerId, jobId, vmId);

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
            result.setStatus("FAILED");
            result.setMessage(e.getMessage());

            rabbitTemplate.convertAndSend(resultExchange, resultRoutingKey, result, m -> {
                m.getMessageProperties().setCorrelationId(fallbackCorr);
                // m.getMessageProperties().setCorrelationId(fallbackCorr.getBytes(StandardCharsets.UTF_8));
                if (jobIdHeader != null) {
                    m.getMessageProperties().setHeader("jobId", jobIdHeader);
                }
                return m;
            });

            // 재큐 없이 DLX로
            throw new AmqpRejectAndDontRequeueException("Listener failed, send to DLX", e);
        }
    }

    /** 다양한 입력(JSON String/byte[], Map, DTO, AMQP Message)을 DTO로 강제 변환 */
    private ProvisionJobMessage coerceToProvisionJobMessage(Object raw) throws Exception {
        log.debug("[Worker:{}] ===== coerceToProvisionJobMessage START =====", workerId);
        log.debug("[Worker:{}] Input type: {}", workerId, raw.getClass().getName());

        if (raw instanceof ProvisionJobMessage pjm) {
            log.debug("[Worker:{}] ✓ Direct ProvisionJobMessage instance", workerId);
            return pjm;
        }

        if (raw instanceof Message amqp) {
            byte[] body = amqp.getBody();
            Map<String, Object> headers = amqp.getMessageProperties().getHeaders();
            String typeId = headers != null && headers.get("__TypeId__") != null
                    ? String.valueOf(headers.get("__TypeId__")) : null;

            log.debug("[Worker:{}] AMQP Message - body length: {}, __TypeId__: {}",
                    workerId, body.length, typeId);

            String bodyStr = new String(body, StandardCharsets.UTF_8);

            // ===== 중요: 실제 JSON 내용 전체 출력 =====
            log.info("[Worker:{}] ===== ACTUAL JSON CONTENT =====", workerId);
            log.info("[Worker:{}] {}", workerId, bodyStr);
            log.info("[Worker:{}] ================================", workerId);

            // 1) 바로 DTO로 파싱 시도
            try {
                ProvisionJobMessage result = objectMapper.readValue(body, ProvisionJobMessage.class);
                log.debug("[Worker:{}] ✓ Direct DTO parse successful", workerId);

                // 파싱 후 필드 확인
                log.debug("[Worker:{}] Parsed - vmName: {}, cpuCores: {}, memoryGb: {}",
                        workerId, result.getVmName(), result.getCpuCores(), result.getMemoryGb());

                return result;
            } catch (MismatchedInputException e) {
                log.warn("[Worker:{}] Direct DTO parse failed (MismatchedInputException): {}", workerId, e.getMessage());
            } catch (Exception e) {
                log.warn("[Worker:{}] Direct DTO parse failed: {}", workerId, e.getMessage());
                log.debug("[Worker:{}] Parse error details:", workerId, e);
            }

            // 2) 문자열 JSON(__TypeId__=java.lang.String 또는 바깥따옴표)
            String s = bodyStr.trim();
            if ("java.lang.String".equals(typeId) || (s.startsWith("\"") && s.endsWith("\""))) {
                try {
                    String inner = objectMapper.readValue(s, String.class); // 바깥 따옴표 벗기기
                    log.debug("[Worker:{}] Nested JSON detected, inner: {}", workerId, inner);
                    ProvisionJobMessage result = objectMapper.readValue(inner, ProvisionJobMessage.class);
                    log.debug("[Worker:{}] ✓ Nested JSON parse successful", workerId);
                    return result;
                } catch (Exception e2) {
                    log.debug("[Worker:{}] Nested JSON parse failed: {}", workerId, e2.toString());
                }
            }

            // 3) Map으로 온 경우
            try {
                Map<String, Object> m = objectMapper.readValue(body, new TypeReference<Map<String, Object>>() {});
                log.info("[Worker:{}] Parsed as Map, keys: {}", workerId, m.keySet());
                log.info("[Worker:{}] Map content: {}", workerId, m);

                ProvisionJobMessage result = objectMapper.convertValue(m, ProvisionJobMessage.class);
                log.debug("[Worker:{}] ✓ Map conversion successful", workerId);

                // 변환 후 필드 확인
                log.debug("[Worker:{}] Converted - vmName: {}, cpuCores: {}, memoryGb: {}",
                        workerId, result.getVmName(), result.getCpuCores(), result.getMemoryGb());

                return result;
            } catch (Exception e3) {
                log.error("[Worker:{}] All parsing attempts failed.", workerId);
                log.error("[Worker:{}] Error: {}", workerId, e3.getMessage(), e3);
                throw new IllegalArgumentException("Unsupported payload; failed to parse JSON", e3);
            }
        }

        if (raw instanceof String s) {
            log.debug("[Worker:{}] String payload: {}", workerId, s);
            // 바깥따옴표로 감싼 문자열 JSON도 처리
            if (s.startsWith("\"") && s.endsWith("\"")) {
                String inner = objectMapper.readValue(s, String.class);
                log.debug("[Worker:{}] Unwrapped nested string", workerId);
                return objectMapper.readValue(inner, ProvisionJobMessage.class);
            }
            return objectMapper.readValue(s, ProvisionJobMessage.class);
        }

        if (raw instanceof byte[] b) {
            log.debug("[Worker:{}] Byte array payload, length: {}", workerId, b.length);
            String content = new String(b, StandardCharsets.UTF_8);
            log.info("[Worker:{}] Byte array content: {}", workerId, content);
            return objectMapper.readValue(b, ProvisionJobMessage.class);
        }

        if (raw instanceof Map<?, ?> m) {
            log.debug("[Worker:{}] Map payload, keys: {}", workerId, m.keySet());
            log.info("[Worker:{}] Map payload: {}", workerId, m);
            return objectMapper.convertValue(m, ProvisionJobMessage.class);
        }

        String preview = String.valueOf(raw);
        if (preview.length() > 200) preview = preview.substring(0, 200) + "...";
        log.error("[Worker:{}] Unsupported payload type: {}, preview: {}",
                workerId, raw.getClass().getName(), preview);
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