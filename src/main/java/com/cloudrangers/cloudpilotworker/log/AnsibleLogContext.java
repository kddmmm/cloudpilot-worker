package com.cloudrangers.cloudpilotworker.log;

import lombok.Data;

/**
 * Ansible 로그 정제를 위한 컨텍스트
 */
@Data
public class AnsibleLogContext {
    private String currentTask;
    private String currentPlay;
    private String currentHost;
    private boolean importantTask;
    private boolean inDebugOutput;
    private boolean failed;
    private int debugLines;

    // 에러 추적용
    private boolean inError;
    private boolean inErrorJson;
    private String errorHost;
    private AnsibleErrorType errorType;

    public void incrementDebugLines() {
        debugLines++;
    }

    public void resetDebugLines() {
        debugLines = 0;
    }

    public void update(String line) {
        if (line.startsWith("PLAY [")) {
            currentPlay = extractBracketContent(line);
            inDebugOutput = false;
        } else if (line.startsWith("TASK [")) {
            currentTask = extractBracketContent(line);
            inDebugOutput = false;

            if (line.contains("Debug") || line.contains("Show")) {
                inDebugOutput = true;
                debugLines = 0;
            }
        } else if (line.matches("^(ok|changed|failed|skipping): \\[.*\\]")) {
            currentHost = extractHost(line);
        }
    }

    private String extractBracketContent(String line) {
        int start = line.indexOf('[') + 1;
        int end = line.indexOf(']');
        return end > start ? line.substring(start, end) : "";
    }

    private String extractHost(String line) {
        int start = line.indexOf('[');
        int end = line.indexOf(']');
        if (start >= 0 && end > start) {
            return line.substring(start + 1, end);
        }
        return "";
    }
}