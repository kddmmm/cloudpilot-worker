package com.cloudrangers.cloudpilotworker.dto;

public class ProvisionResultMessage {
    private String jobId;
    private String status;  // SUCCEEDED / FAILED
    private String vmId;    // 성공 시 VM 식별자(모의)
    private String message; // 상세 메시지

    public ProvisionResultMessage() {}

    public ProvisionResultMessage(String jobId, String status, String vmId, String message) {
        this.jobId = jobId;
        this.status = status;
        this.vmId = vmId;
        this.message = message;
    }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getVmId() { return vmId; }
    public void setVmId(String vmId) { this.vmId = vmId; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    @Override
    public String toString() {
        return "ProvisionResultMessage{" +
                "jobId='" + jobId + '\'' +
                ", status='" + status + '\'' +
                ", vmId='" + vmId + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}