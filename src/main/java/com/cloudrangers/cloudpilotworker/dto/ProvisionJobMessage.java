package com.cloudrangers.cloudpilotworker.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProvisionJobMessage {

    // 기본 필드
    private String jobId;
    private Object providerType;
    private Long zoneId;

    private Long userId;
    private Long teamId;

    private Integer vmCount;
    private String vmName;
    private Integer cpuCores;
    private Integer memoryGb;
    private Integer diskGb;

    private Map<String, String> tags;
    private Map<String, Object> additionalConfig;

    // 추가 스펙
    private TemplateRef template;
    private OsSpec os;
    private NetSpec net;
    private PropertiesSpec properties;

    // ---------- Nested Types ----------

    @Data
    public static class TemplateRef {
        private String itemName;
        private String templateMoid;
        private String templateDatastore;
        private String guestId;
        private String contentLibraryName;
        private String uuid;
    }

    @Data
    public static class OsSpec {
        private String family;
        private String version;
        private String variant;
        private String arch;
    }

    @Data
    public static class NetSpec {
        private String mode;     // DHCP | STATIC
        private String iface;
        private Ipv4 ipv4;
        private List<String> dns;

        @Data
        public static class Ipv4 {
            private String address;
            private Integer prefix;
            private String gateway;
        }
    }

    @Data
    public static class PropertiesSpec {
        private String hostname;
        private String timezone;
        private List<User> users;
        private List<String> packages;
        private List<FileSpec> files;
        private List<String> runcmd;
        private WinSpec win;

        @Data
        public static class User {
            private String name;
            private String sudo;
            private String shell;
            private List<String> ssh_authorized_keys;
        }

        @Data
        public static class FileSpec {
            private String path;
            private String content;
            private String owner;
            private String perm;
        }

        @Data
        public static class WinSpec {
            private String admin_password;
            private String join_domain;
            private String domain_admin_user;
            private String domain_admin_password;
            private List<String> firstboot_ps;
        }
    }
}
