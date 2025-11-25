package com.cloudrangers.cloudpilotworker.log;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Ansible 로그 실시간 정제
 */
@Component
@Slf4j
public class AnsibleLogRefiner {

    private static final Pattern IP_PATTERN =
            Pattern.compile("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b");

    /**
     * 실시간으로 Ansible 로그 라인을 정제
     * @return 정제된 로그 라인 (null이면 스킵)
     */
    public String refineLine(String line, AnsibleLogContext context) {
        if (line == null) return null;

        // 1. 타임스탬프/로거 제거
        line = line.replaceAll("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}.*?\\[Ansible-Log\\]\\s*", "");
        String trimmed = line.trim();

        // 2. 에러 처리가 우선
        if (context.isInError()) {
            return captureErrorContext(trimmed, context);
        }

        // Ansible 실행 에러 (ERROR!로 시작)
        if (trimmed.startsWith("ERROR!")) {
            context.setInError(true);
            context.setErrorType(AnsibleErrorType.SYNTAX_ERROR);

            StringBuilder error = new StringBuilder("\n❌ ============ ANSIBLE ERROR ============\n");
            error.append("    ❌ ").append(maskSensitiveInfo(trimmed)).append("\n");

            return error.toString();
        }

        // Task 실패
        if (trimmed.startsWith("failed:") || trimmed.contains("fatal:")) {
            context.setInError(true);
            context.setErrorHost(extractHost(trimmed));
            return "\n❌ ============ TASK FAILED ============";
        }

        // Host 연결 불가
        if (trimmed.contains("unreachable:")) {
            return formatUnreachableError(trimmed, context);
        }

        // 3. 불필요한 라인 스킵
        if (shouldSkip(trimmed, context)) {
            return null;
        }

        // 4. 컨텍스트 업데이트
        context.update(trimmed);

        // 5. 라인 처리
        return processLine(trimmed, context);
    }

    /**
     * 스킵할 라인 판단
     */
    private boolean shouldSkip(String line, AnsibleLogContext context) {
        if (line.isEmpty()) return true;

        // 구분선
        if (line.matches("^\\*+$")) return true;

        // Gathering Facts
        if (line.contains("Gathering Facts")) return true;

        // ok 상태 (changed가 아닌 경우)
        if (line.startsWith("ok:") && !context.isImportantTask()) {
            return true;
        }

        // 디버그 출력 제한
        if (context.isInDebugOutput() && line.startsWith("    \"")) {
            context.incrementDebugLines();
            if (context.getDebugLines() > 2) {
                return true;
            }
        }

        // JSON 구조
        if (line.matches("^[\\[\\]{},]*$")) {
            return true;
        }

        return false;
    }

    /**
     * 라인 처리 및 포맷팅
     */
    private String processLine(String line, AnsibleLogContext context) {
        // PLAY 헤더
        if (line.startsWith("PLAY [")) {
            String playName = context.getCurrentPlay();
            return String.format("\n=== 📦 PLAY: %s ===", playName);
        }

        // TASK (중요한 것만)
        if (line.startsWith("TASK [")) {
            String taskName = context.getCurrentTask();

            if (isImportantTask(taskName)) {
                context.setImportantTask(true);
                return String.format("  → %s", taskName);
            }
            context.setImportantTask(false);
            return null;
        }

        // changed 상태
        if (line.startsWith("changed:")) {
            String host = context.getCurrentHost();
            return String.format("    ✅ Changed: %s - %s",
                    maskIp(host), context.getCurrentTask());
        }

        // failed 상태
        if (line.startsWith("failed:")) {
            context.setFailed(true);
            String host = context.getCurrentHost();
            return String.format("    ❌ FAILED: %s - %s",
                    maskIp(host), context.getCurrentTask());
        }

        // PLAY RECAP
        if (line.startsWith("PLAY RECAP")) {
            return "\n=== 📊 SUMMARY ===";
        }

        if (line.matches(".*:\\s+ok=\\d+.*")) {
            return formatRecap(line);
        }

        // Debug output
        if (line.startsWith("ok:") && context.isInDebugOutput()) {
            if (line.contains("\"msg\":")) {
                String msg = extractJsonValue(line, "msg");
                return String.format("    ℹ %s", msg);
            }
        }

        // 에러 메시지
        if (line.contains("ERROR") || line.contains("FAILED") || line.contains("fatal:")) {
            return "    ❌ " + line;
        }

        return null;
    }

    /**
     * 에러 컨텍스트 캡처
     */
    private String captureErrorContext(String line, AnsibleLogContext context) {
        StringBuilder error = new StringBuilder();

        // 에러 종료 조건: 새로운 PLAY 또는 TASK 시작
        if (line.startsWith("PLAY [") || line.startsWith("TASK [")) {
            context.setInError(false);
            context.setInErrorJson(false);
            return buildErrorSummary(context);
        }

        // 빈 라인 연속 2개면 종료
        if (line.isEmpty()) {
            context.incrementDebugLines(); // 카운터 용도로 재사용
            if (context.getDebugLines() >= 2) {
                context.setInError(false);
                context.setInErrorJson(false);
                context.resetDebugLines();
                return buildErrorSummary(context);
            }
            return null;
        } else {
            context.resetDebugLines();
        }

        // 에러 JSON 시작
        if (line.startsWith("{") || line.equals("=>")) {
            context.setInErrorJson(true);
            return null;
        }

        // JSON 에러 메시지 추출
        if (line.contains("\"msg\":")) {
            String msg = extractJsonValue(line, "msg");
            error.append(String.format("\n📛 Task: %s\n", context.getCurrentTask()));
            error.append(String.format("📄 Error Message: %s\n", msg));

            AnsibleErrorType type = classifyError(msg);
            context.setErrorType(type);
            error.append(String.format("🏷️  Error Type: %s\n", type.getDisplayName()));

            return error.toString();
        }

        // stderr
        if (line.contains("\"stderr\":") || line.contains("STDERR:")) {
            String stderr = extractStderr(line);
            if (!stderr.isEmpty()) {
                return String.format("  └─ stderr: %s\n", stderr);
            }
        }

        // rc (return code)
        if (line.contains("\"rc\":")) {
            String rc = extractJsonValue(line, "rc");
            return String.format("  └─ return code: %s\n", rc);
        }

        // cmd
        if (line.contains("\"cmd\":")) {
            String cmd = extractJsonValue(line, "cmd");
            return String.format("  └─ command: %s\n", maskSensitiveInCommand(cmd));
        }

        // 일반 텍스트 에러 컨텍스트 (ERROR! 이후의 라인들)
        // 빈 라인 2개가 나올 때까지 모든 라인 수집 (단, 불필요한 것만 필터링)

        // 완전히 스킵할 라인 (노이즈)
        if (line.matches("^=+$") || line.matches("^-+$")) {
            return null; // 구분선
        }

        // 중요한 키워드를 포함한 라인은 강조
        if (line.contains("appears to be") || line.contains("offending line")) {
            return "\n    " + maskSensitiveInfo(line) + "\n";
        }

        if (line.contains("^ here") || line.contains("^~~~")) {
            return "    " + line + " ← HERE\n";
        }

        // 나머지는 모두 수집 (들여쓰기 추가)
        if (!line.isEmpty()) {
            // 이미 들여쓰기가 있는 라인
            if (line.startsWith("  ") || line.startsWith("\t")) {
                return "    " + maskSensitiveInfo(line.trim()) + "\n";
            }
            // 들여쓰기 없는 라인 (주석, YAML 구조 등)
            else {
                return "    " + maskSensitiveInfo(line) + "\n";
            }
        }

        return null;
    }

    /**
     * Unreachable 에러
     */
    private String formatUnreachableError(String line, AnsibleLogContext context) {
        String host = extractHost(line);
        context.setErrorType(AnsibleErrorType.UNREACHABLE);

        StringBuilder error = new StringBuilder();
        error.append("\n❌ ============ HOST UNREACHABLE ============\n");
        error.append(String.format("📛 Host: %s\n", maskIp(host)));
        error.append("📄 Error: Cannot establish SSH connection\n");
        error.append("💡 Suggested Action: Check network connectivity and SSH access\n");
        error.append("=============================================\n");

        return error.toString();
    }

    /**
     * Ansible 에러 타입 분류
     */
    private AnsibleErrorType classifyError(String msg) {
        String lower = msg.toLowerCase();

        if (lower.contains("permission denied") || lower.contains("sudo")) {
            return AnsibleErrorType.PERMISSION_DENIED;
        }
        if (lower.contains("not found") || lower.contains("no such file")) {
            return AnsibleErrorType.FILE_NOT_FOUND;
        }
        if (lower.contains("connection") || lower.contains("timeout")) {
            return AnsibleErrorType.CONNECTION_ERROR;
        }
        if (lower.contains("failed to start") || lower.contains("service")) {
            return AnsibleErrorType.SERVICE_FAILED;
        }
        if (lower.contains("package") || lower.contains("install")) {
            return AnsibleErrorType.PACKAGE_ERROR;
        }
        if (lower.contains("syntax") || lower.contains("invalid")) {
            return AnsibleErrorType.SYNTAX_ERROR;
        }

        return AnsibleErrorType.UNKNOWN;
    }

    /**
     * 에러 요약
     */
    private String buildErrorSummary(AnsibleLogContext context) {
        StringBuilder summary = new StringBuilder();

        summary.append("\n📊 Error Analysis:\n");
        summary.append(String.format("  • Type: %s\n",
                context.getErrorType().getDisplayName()));
        summary.append(String.format("  • Host: %s\n",
                maskIp(context.getErrorHost())));
        summary.append(String.format("  • Failed Task: %s\n",
                context.getCurrentTask()));
        summary.append(String.format("  • Play: %s\n",
                context.getCurrentPlay()));

        summary.append(String.format("\n💡 Suggested Action: %s\n",
                context.getErrorType().getSuggestedAction()));

        summary.append("=============================================\n");

        return summary.toString();
    }

    // ===== 유틸리티 메서드 =====

    private boolean isImportantTask(String taskName) {
        return taskName.contains("Install") ||
                taskName.contains("Download") ||
                taskName.contains("Configure") ||
                taskName.contains("Start") ||
                taskName.contains("Update") ||
                taskName.contains("Deploy") ||
                taskName.contains("Register") ||
                taskName.matches(".*Service$") ||
                taskName.contains("Firewall");
    }

    private String formatRecap(String line) {
        Pattern p = Pattern.compile("(\\S+)\\s*:\\s+ok=(\\d+)\\s+changed=(\\d+).*failed=(\\d+)");
        Matcher m = p.matcher(line);

        if (m.find()) {
            String host = maskIp(m.group(1));
            int changed = Integer.parseInt(m.group(3));
            int failed = Integer.parseInt(m.group(4));

            String status = failed > 0 ? "❌" : "✅";
            return String.format("  %s %s: %d changed, %d failed",
                    status, host, changed, failed);
        }

        return "  " + line;
    }

    private String extractHost(String line) {
        Pattern p = Pattern.compile("\\[(.*?)\\]");
        Matcher m = p.matcher(line);
        return m.find() ? m.group(1) : "unknown";
    }

    private String extractJsonValue(String line, String key) {
        Pattern p = Pattern.compile("\"" + key + "\":\\s*\"?([^\"\\},]+)\"?");
        Matcher m = p.matcher(line);
        return m.find() ? m.group(1).trim() : "";
    }

    private String extractStderr(String line) {
        if (line.contains("STDERR:")) {
            return line.substring(line.indexOf("STDERR:") + 7).trim();
        }
        return extractJsonValue(line, "stderr");
    }

    private String maskIp(String ip) {
        return IP_PATTERN.matcher(ip).replaceAll("***IP***");
    }

    private String maskSensitiveInfo(String line) {
        // IP 마스킹
        line = IP_PATTERN.matcher(line).replaceAll("***IP***");

        // 비밀번호 마스킹
        line = line.replaceAll("(password|passwd|pwd)\\s*[:=]\\s*\\S+", "$1: ***");

        // 파일 경로 간소화 (전체 경로 표시는 보안상 좋지 않음)
        line = line.replaceAll("/etc/ansible/roles/([^/]+)/.*", "/etc/ansible/roles/$1/...");

        return line;
    }

    private String maskSensitiveInCommand(String cmd) {
        return cmd
                .replaceAll("password[=:]\\S+", "password=***")
                .replaceAll("--password\\s+\\S+", "--password ***")
                .replaceAll("-p\\s+\\S+", "-p ***");
    }
}