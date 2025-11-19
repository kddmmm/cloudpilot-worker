package com.cloudrangers.cloudpilotworker.dto;

import java.time.OffsetDateTime;
import java.util.List;

public class ProvisionResultMessage {

    public enum EventType {
        LOG,        // Terraform 진행 로그
        SUCCESS,    // VM 생성 완료
        ERROR       // 비즈니스 에러
    }

    private String jobId;
    private EventType eventType;

    // 기존 필드 (호환용)
    private String status;        // SUCCEEDED / FAILED / LOG
    private String vmId;          // 단일 VM id (첫 번째 VM 등)
    private String message;       // 로그/에러/요약

    // 공통 메타
    private String step;          // terraform_init / terraform_apply ...
    private OffsetDateTime timestamp;

    // SUCCESS 에서만 채우는 VM 목록
    private List<InstanceInfo> instances;

    public ProvisionResultMessage() {}

    public ProvisionResultMessage(String jobId, String status, String vmId, String message) {
        this.jobId = jobId;
        this.status = status;
        this.vmId = vmId;
        this.message = message;
    }

    // ===== getter/setter =====
    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public EventType getEventType() { return eventType; }
    public void setEventType(EventType eventType) { this.eventType = eventType; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public String getVmId() { return vmId; }
    public void setVmId(String vmId) { this.vmId = vmId; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getStep() { return step; }
    public void setStep(String step) { this.step = step; }

    public OffsetDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(OffsetDateTime timestamp) { this.timestamp = timestamp; }

    public List<InstanceInfo> getInstances() { return instances; }
    public void setInstances(List<InstanceInfo> instances) { this.instances = instances; }

    @Override
    public String toString() {
        return "ProvisionResultMessage{" +
                "jobId='" + jobId + '\'' +
                ", eventType=" + eventType +
                ", status='" + status + '\'' +
                ", vmId='" + vmId + '\'' +
                ", message='" + message + '\'' +
                ", step='" + step + '\'' +
                ", timestamp=" + timestamp +
                ", instances=" + instances +
                '}';
    }

    // ===== VM 한 개 정보 =====
    public static class InstanceInfo {
        private String name;
        private String externalId;
        private Long zoneId;
        private String providerType;
        private Integer cpuCores;
        private Integer memoryGb;
        private Integer diskGb;
        private String ipAddress;
        private String osType;

        public InstanceInfo() {}

        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public String getExternalId() { return externalId; }
        public void setExternalId(String externalId) { this.externalId = externalId; }

        public Long getZoneId() { return zoneId; }
        public void setZoneId(Long zoneId) { this.zoneId = zoneId; }

        public String getProviderType() { return providerType; }
        public void setProviderType(String providerType) { this.providerType = providerType; }

        public Integer getCpuCores() { return cpuCores; }
        public void setCpuCores(Integer cpuCores) { this.cpuCores = cpuCores; }

        public Integer getMemoryGb() { return memoryGb; }
        public void setMemoryGb(Integer memoryGb) { this.memoryGb = memoryGb; }

        public Integer getDiskGb() { return diskGb; }
        public void setDiskGb(Integer diskGb) { this.diskGb = diskGb; }

        public String getIpAddress() { return ipAddress; }
        public void setIpAddress(String ipAddress) { this.ipAddress = ipAddress; }

        public String getOsType() { return osType; }
        public void setOsType(String osType) { this.osType = osType; }

        @Override
        public String toString() {
            return "InstanceInfo{" +
                    "name='" + name + '\'' +
                    ", externalId='" + externalId + '\'' +
                    ", zoneId=" + zoneId +
                    ", providerType='" + providerType + '\'' +
                    ", cpuCores=" + cpuCores +
                    ", memoryGb=" + memoryGb +
                    ", diskGb=" + diskGb +
                    ", ipAddress='" + ipAddress + '\'' +
                    ", osType='" + osType + '\'' +
                    '}';
        }
    }
}
