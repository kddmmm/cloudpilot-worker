package com.cloudrangers.cloudpilotworker.log;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Terraform 로그 실시간 정제
 */
@Component
@Slf4j
public class TerraformLogRefiner {

    private static final Pattern TIMESTAMP_PATTERN =
            Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}");

    private static final Pattern IP_PATTERN =
            Pattern.compile("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b");

    private static final Pattern UUID_PATTERN =
            Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

    /**
     * 실시간으로 Terraform 로그 라인을 정제
     * @return 정제된 로그 라인 (null이면 스킵)
     */
    public String refineLine(String line, TerraformLogContext context) {
        if (line == null) return null;

        // 1. 타임스탬프 제거
        line = TIMESTAMP_PATTERN.matcher(line).replaceAll("");

        // 2. 스레드명/로거명 제거
        line = line.replaceAll("^\\[.*?\\]\\s+(INFO|DEBUG|WARN|ERROR)\\s+\\S+\\s+-\\s+", "");

        // 3. [terraform] 프리픽스 정리
        line = line.replaceAll("^\\[terraform\\]\\s+", "");
        line = line.replaceAll("^\\[terraform-output\\]\\s+", "");
        line = line.trim();

        // 4. 에러 처리가 우선
        if (context.isInError()) {
            return captureErrorContext(line, context);
        }

        // 에러 감지 시 즉시 처리
        if (line.toLowerCase().contains("error:") || line.toLowerCase().contains("error ")) {
            context.setInError(true);
            context.setErrorStartLine(line);

            // 에러 헤더 + 첫 번째 에러 메시지 함께 반환
            StringBuilder error = new StringBuilder("\n❌ ============ ERROR DETECTED ============\n");

            // 에러 메시지 추출 및 타입 분류
            if (line.toLowerCase().startsWith("error:")) {
                String msg = line.substring(line.indexOf(":") + 1).trim();
                TerraformErrorType type = classifyError(msg);
                context.setErrorType(type);
                context.getErrorLines().add(line);

                error.append(String.format("\n📛 Error Type: %s\n", type.getDisplayName()));
                error.append(String.format("📄 Message: %s\n", maskSensitiveInfo(msg)));
            } else {
                // "Error" 단어를 포함하지만 시작하지 않는 경우
                context.getErrorLines().add(line);
                error.append("  ").append(maskSensitiveInfo(line)).append("\n");
            }

            return error.toString();
        }

        // 5. 불필요한 라인 필터링
        if (shouldSkip(line, context)) {
            return null;
        }

        // 6. 컨텍스트 업데이트
        context.update(line);

        // 7. 라인 분류 및 정제
        return processLine(line, context);
    }

    /**
     * 스킵할 라인 판단
     */
    private boolean shouldSkip(String line, TerraformLogContext context) {
        if (line.isEmpty()) return true;

        // Still destroying 반복 (생성은 10초 단위로 모두 노출, 삭제는 그대로 스킵)
        if (line.contains("Still destroying")
                && !context.isFirstStillCreating() && !context.isLongRunning()) {
            return true;
        }

        // Reading/Refreshing state
        if (line.contains("Reading...") ||
                line.contains("Refreshing state") ||
                line.contains("Read complete")) {
            return true;
        }

        // Provider 이동 경고
        if (line.contains("For users on Terraform 0.13")) {
            return true;
        }

        // Plan detail 중 속성들
        if (context.isInPlanDetail() && line.matches("\\s*[+~]?\\s*\\w+\\s+[:=].*")) {
            context.incrementSkippedAttributes();
            if (context.getSkippedAttributes() > 5) {
                return true;
            }
        }

        // Output JSON의 type 정의
        if (context.isInOutputJson() &&
                (line.contains("\"type\":") || line.contains("\"sensitive\": false"))) {
            return true;
        }

        return false;
    }

    /**
     * 라인 처리 및 포맷팅
     */
    private String processLine(String line, TerraformLogContext context) {
        // 단계 헤더
        if (line.contains("Initializing the backend")) {
            return "\n=== INIT: Backend Initialization ===";
        }
        if (line.contains("Terraform will perform the following actions")) {
            context.setInPlanDetail(true);
            return "\n=== PLAN: Resource Changes ===";
        }

        // VM 생성 시작/완료
        if (line.startsWith("vsphere_virtual_machine")) {
            if (line.contains("Creating...")) {
                return "→ Creating VM: " + (context.getVmName() != null ? context.getVmName() : "Unknown");
            }
            if (line.contains("Creation complete")) {
                String duration = extractDuration(line);
                String vmId = extractVmId(line);
                return String.format("✅ VM Created (ID: %s) in %s", maskUuid(vmId), duration);
            }
        }

        // Plan 요약
        if (line.startsWith("Plan:")) {
            context.setInPlanDetail(false);
            return "\n" + line;
        }

        // Apply 완료
        if (line.startsWith("Apply complete!")) {
            return "\n✅ " + line;
        }

        // Output 요약
        if (context.isInOutputJson()) {
            if (line.contains("\"value\": \"") && line.contains("ip")) {
                String ip = extractQuotedValue(line);
                return String.format("  • IP Address: %s", ip);
            }
        }

        // 초기화 완료
        if (line.contains("Terraform has been successfully initialized")) {
            return "✓ Terraform Initialized";
        }

        // Still creating (10초 단위로 모두 출력)
        if (line.contains("Still creating")) {
            String elapsed = extractElapsed(line);
            if (context.isFirstStillCreating()) {
                context.setFirstStillCreating(false);
                return String.format("  ... Creating (elapsed: %s)", elapsed);
            }
            // 이후에는 매번 10초마다 들어오는 로그를 그대로 노출
            return String.format("  ... Still creating (%s)", elapsed);
        }

        // 중요 메시지만 통과
        if (isCriticalMessage(line)) {
            return "  " + line;
        }

        return null;
    }

    /**
     * 에러 컨텍스트 캡처
     */
    private String captureErrorContext(String line, TerraformLogContext context) {
        context.getErrorLines().add(line);

        // "Error:" 라인은 이미 처리됨 (refineLine에서)
        // 여기서는 나머지 에러 컨텍스트만 처리

        // 에러 상세 (with, on, at 등)
        if (line.startsWith("on ") || line.startsWith("with ") ||
                line.startsWith("at ") || line.startsWith("in ")) {
            return "  └─ " + maskSensitiveInfo(line);
        }

        // 에러 종료
        if (line.isEmpty()) {
            context.incrementEmptyLines();
            if (context.getEmptyLines() >= 2) {
                context.setInError(false);
                context.resetEmptyLines();
                return buildErrorSummary(context);
            }
        }

        return "  " + maskSensitiveInfo(line);
    }

    /**
     * 에러 타입 분류
     */
    private TerraformErrorType classifyError(String msg) {
        String lower = msg.toLowerCase();

        if (lower.contains("timeout") || lower.contains("timed out")) {
            return TerraformErrorType.TIMEOUT;
        }
        if (lower.contains("authentication") || lower.contains("unauthorized") ||
                lower.contains("credentials")) {
            return TerraformErrorType.AUTH_FAILED;
        }
        if (lower.contains("not found") || lower.contains("doesn't exist")) {
            return TerraformErrorType.RESOURCE_NOT_FOUND;
        }
        if (lower.contains("already exists") || lower.contains("duplicate")) {
            return TerraformErrorType.RESOURCE_CONFLICT;
        }
        if (lower.contains("network") || lower.contains("connection")) {
            return TerraformErrorType.NETWORK_ERROR;
        }
        if (lower.contains("permission") || lower.contains("forbidden")) {
            return TerraformErrorType.PERMISSION_DENIED;
        }
        if (lower.contains("invalid") || lower.contains("malformed")) {
            return TerraformErrorType.INVALID_CONFIG;
        }
        if (lower.contains("quota") || lower.contains("limit exceeded")) {
            return TerraformErrorType.QUOTA_EXCEEDED;
        }

        return TerraformErrorType.UNKNOWN;
    }

    /**
     * 에러 요약 생성
     */
    private String buildErrorSummary(TerraformLogContext context) {
        StringBuilder summary = new StringBuilder();

        // 🔒 null-safe 처리 (추가 방어는 필요하면 여기서 해도 됨)
        TerraformErrorType type = null;
        if (context != null) {
            type = context.getErrorType();
        }

        summary.append("\n📊 Error Analysis:\n");
        summary.append(String.format("  • Type: %s\n", context.getErrorType().getDisplayName()));
        summary.append(String.format("  • Stage: %s\n", context.getCurrentStage()));
        summary.append(String.format("  • Resource: %s\n",
                context.getFailedResource() != null ? context.getFailedResource() : "unknown"));
        summary.append(String.format("  • Action: %s\n",
                context.getCurrentAction() != null ? context.getCurrentAction() : "unknown"));

        if (context.hasRelatedConfig()) {
            summary.append("  • Related Config:\n");
            context.getRelatedConfig().forEach(config ->
                    summary.append(String.format("    - %s\n", config)));
        }

        summary.append(String.format("\n💡 Suggested Action: %s\n",
                context.getErrorType().getSuggestedAction()));

        summary.append("==========================================\n");

        return summary.toString();
    }

    // ===== 유틸리티 메서드 =====

    private boolean isCriticalMessage(String line) {
        return line.contains("complete") ||
                line.contains("Success") ||
                line.contains("configured") ||
                line.contains("WARNING");
    }

    private String extractDuration(String line) {
        Pattern p = Pattern.compile("after\\s+([\\dmsh]+)");
        Matcher m = p.matcher(line);
        return m.find() ? m.group(1) : "unknown";
    }

    private String extractVmId(String line) {
        Pattern p = Pattern.compile("\\[id=([^\\]]+)\\]");
        Matcher m = p.matcher(line);
        return m.find() ? m.group(1) : "";
    }

    private String extractElapsed(String line) {
        Pattern p = Pattern.compile("\\[(\\d+m\\d+s) elapsed\\]");
        Matcher m = p.matcher(line);
        return m.find() ? m.group(1) : "";
    }

    private String extractQuotedValue(String line) {
        Pattern p = Pattern.compile("\"value\":\\s*\"([^\"]+)\"");
        Matcher m = p.matcher(line);
        return m.find() ? m.group(1) : "";
    }

    private String maskUuid(String uuid) {
        if (uuid.length() < 13) return uuid;
        return uuid.substring(0, 8) + "..." + uuid.substring(uuid.length() - 4);
    }

    private String maskSensitiveInfo(String line) {
        line = line.replaceAll("(password|token|secret|key)\\s*[:=]\\s*\\S+", "$1: ***");
        line = IP_PATTERN.matcher(line).replaceAll("***IP***");
        line = UUID_PATTERN.matcher(line).replaceAll("***UUID***");
        return line;
    }
}
