package com.cloudrangers.cloudpilotworker.log;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * Terraform 로그 정제를 위한 컨텍스트
 */
@Data
public class TerraformLogContext {
    private String jobId;
    private String vmName;
    private String currentStage = "init";
    private String currentAction;
    private boolean inPlanDetail;
    private boolean inOutputJson;
    private boolean firstStillCreating = true;
    private boolean capturingIp;
    private int skippedAttributes;
    private long startTime = System.currentTimeMillis();

    // 에러 추적용
    private boolean inError;
    private String errorStartLine;
    private TerraformErrorType errorType;
    private int emptyLines;
    private int stackLines;
    private String failedResource;
    private List<String> relatedConfig = new ArrayList<>();
    private List<String> errorLines = new ArrayList<>();

    public void incrementEmptyLines() {
        emptyLines++;
    }

    public void resetEmptyLines() {
        emptyLines = 0;
    }

    public void incrementStackLines() {
        stackLines++;
    }

    public void incrementSkippedAttributes() {
        skippedAttributes++;
    }

    public boolean isLongRunning() {
        return (System.currentTimeMillis() - startTime) > 120000; // 2분 이상
    }

    public boolean hasRelatedConfig() {
        return !relatedConfig.isEmpty();
    }

    public void update(String line) {
        // VM 이름 추출
        if (line.contains("vmName=") || line.contains("name=")) {
            String[] parts = line.split("name[=:]\\s*");
            if (parts.length > 1) {
                String potential = parts[1].split("[,\\s]")[0].replaceAll("[\"']", "");
                if (!potential.isEmpty()) {
                    vmName = potential;
                }
            }
        }

        // 현재 단계 추적
        if (line.contains("Initializing")) currentStage = "init";
        else if (line.contains("Terraform will perform")) currentStage = "plan";
        else if (line.contains("Creating...") || line.contains("Applying")) currentStage = "apply";
        else if (line.contains("Destroying...")) currentStage = "destroy";

        // 현재 액션 추적
        if (line.contains("Creating...")) currentAction = "creating";
        else if (line.contains("Modifying...")) currentAction = "modifying";
        else if (line.contains("Destroying...")) currentAction = "destroying";

        // Output JSON 진입
        if (line.contains("terraform output -json")) {
            inOutputJson = true;
        }

        if (line.contains("Apply complete!")) {
            inOutputJson = false;
        }

        // 실패한 리소스 추출
        if (line.contains("vsphere_virtual_machine")) {
            int idx = line.indexOf("vsphere_virtual_machine");
            String sub = line.substring(idx);
            String[] tokens = sub.split("[\\[\\]\\s:,]");
            if (tokens.length > 0) {
                failedResource = tokens[0];
            }
        }
    }
}
