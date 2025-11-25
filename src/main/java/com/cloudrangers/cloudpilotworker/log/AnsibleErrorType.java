package com.cloudrangers.cloudpilotworker.log;

/**
 * Ansible 에러 타입 분류
 */
public enum AnsibleErrorType {
    UNREACHABLE("Host Unreachable",
            "Verify SSH connectivity and check firewall rules"),
    PERMISSION_DENIED("Permission Denied",
            "Grant sudo privileges or run with appropriate user"),
    FILE_NOT_FOUND("File/Directory Not Found",
            "Check if required files exist or update file paths"),
    CONNECTION_ERROR("Connection Error",
            "Verify network connectivity and SSH configuration"),
    SERVICE_FAILED("Service Start/Stop Failed",
            "Check service logs with 'journalctl -xe' or 'systemctl status'"),
    PACKAGE_ERROR("Package Installation Failed",
            "Verify package repository availability and package name"),
    SYNTAX_ERROR("Syntax/Configuration Error",
            "Review playbook syntax with 'ansible-playbook --syntax-check'"),
    UNKNOWN("Unknown Error",
            "Review error details and check Ansible documentation");

    private final String displayName;
    private final String suggestedAction;

    AnsibleErrorType(String displayName, String suggestedAction) {
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
