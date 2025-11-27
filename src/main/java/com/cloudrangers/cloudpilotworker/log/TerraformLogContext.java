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

    /**
     * ❗ 기본값을 UNKNOWN으로 설정해서
     *    getErrorType()이 절대 null을 반환하지 않도록 함
     */
    private TerraformErrorType errorType = TerraformErrorType.UNKNOWN;

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

    /**
     * ❗ Lombok @Data가 getter/setter를 만들어주지만,
     *    우리가 직접 정의하면 이 메서드들이 우선 사용됨.
     *    -> 항상 null 대신 UNKNOWN을 반환/설정하도록 강제.
     */
    public TerraformErrorType getErrorType() {
        return errorType != null ? errorType : TerraformErrorType.UNKNOWN;
    }

    public void setErrorType(TerraformErrorType errorType) {
        this.errorType = (errorType != null) ? errorType : TerraformErrorType.UNKNOWN;
    }

    public void update(String line) {
        // VM 이름 추출 (다양한 패턴 지원)
        if (vmName == null || vmName.equals("Unknown")) {
            // 패턴 1: + name = "test-vm"
            if (line.contains("+ name") || line.contains("name =")) {
                java.util.regex.Pattern p = java.util.regex.Pattern.compile("[+\\s]name\\s*=\\s*\"([^\"]+)\"");
                java.util.regex.Matcher m = p.matcher(line);
                if (m.find()) {
                    vmName = m.group(1);
                }
            }
            // 패턴 2: name = "test-vm" (따옴표 없이)
            else if (line.matches(".*\\bname\\s*=\\s*\\S+.*")) {
                java.util.regex.Pattern p = java.util.regex.Pattern.compile("\\bname\\s*=\\s*\"?([^\\s\",]+)\"?");
                java.util.regex.Matcher m = p.matcher(line);
                if (m.find()) {
                    String name = m.group(1);
                    if (!name.equals("var.vm_name") && !name.startsWith("${")) {
                        vmName = name;
                    }
                }
            }
            // 패턴 3: "value": "test-vm" (output에서)
            else if (line.contains("\"value\":")) {
                java.util.regex.Pattern p = java.util.regex.Pattern.compile("\"value\":\\s*\"([^\"]+)\"");
                java.util.regex.Matcher m = p.matcher(line);
                if (m.find() && !m.group(1).matches("\\d+\\.\\d+\\.\\d+\\.\\d+")) { // IP가 아닌 경우
                    vmName = m.group(1);
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
