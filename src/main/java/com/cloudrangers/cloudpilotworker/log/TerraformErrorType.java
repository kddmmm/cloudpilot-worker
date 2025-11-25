package com.cloudrangers.cloudpilotworker.log;

/**
 * Terraform 에러 타입 분류
 */
public enum TerraformErrorType {
    TIMEOUT("Connection/Operation Timeout",
            "Increase timeout values or check network connectivity to vSphere"),
    AUTH_FAILED("Authentication Failed",
            "Verify vSphere credentials and permissions"),
    RESOURCE_NOT_FOUND("Resource Not Found",
            "Check if template/datastore/network exists in vSphere"),
    RESOURCE_CONFLICT("Resource Already Exists",
            "VM with same name may already exist. Check and remove if necessary"),
    NETWORK_ERROR("Network Connection Error",
            "Verify network configuration and vSphere connectivity"),
    PERMISSION_DENIED("Permission Denied",
            "Grant necessary permissions to the service account"),
    INVALID_CONFIG("Invalid Configuration",
            "Review terraform configuration for syntax errors"),
    QUOTA_EXCEEDED("Quota/Limit Exceeded",
            "Check resource quotas and limits in vSphere"),
    UNKNOWN("Unknown Error",
            "Review error details and check Terraform documentation");

    private final String displayName;
    private final String suggestedAction;

    TerraformErrorType(String displayName, String suggestedAction) {
        this.displayName = displayName;
        this.suggestedAction = suggestedAction;
    }

    public String getDisplayName() {
        return displayName;
    }

    public String getSuggestedAction() {
        return suggestedAction;
    }
}
