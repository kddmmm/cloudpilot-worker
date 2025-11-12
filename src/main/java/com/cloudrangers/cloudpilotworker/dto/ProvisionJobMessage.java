package com.cloudrangers.cloudpilotworker.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProvisionJobMessage {

    // 기존 필드(유지)
    private String jobId;
    private Object providerType;
    private Long zoneId;

    private Integer vmCount;
    private String vmName;
    private Integer cpuCores;
    private Integer memoryGb;
    private Integer diskGb;

    private Map<String, String> tags;
    private Map<String, Object> additionalConfig;

    // ★ 추가 필드(API와 동일)
    private TemplateRef template;
    private OsSpec os;
    private NetSpec net;
    private PropertiesSpec properties;

    // ---------- Nested Types ----------
    @Data
    public static class TemplateRef {
        private String itemName;           // os_image.template_name
        private String templateMoid;       // os_image.template_moid (있으면 우선)
        private String templateDatastore;  // os_image.template_datastore
        private String guestId;            // os_image.guest_id
        private String contentLibraryName; // 선택
        private String uuid;               // (선택) 미래에 UUID를 직접 줄 수도 있음
    }

    @Data
    public static class OsSpec {
        private String family;   // ubuntu | rocky | windows
        private String version;  // "22.04" 등
        private String variant;  // minimal 등
        private String arch;     // x86_64 등
    }

    @Data
    public static class NetSpec {
        private String mode;     // DHCP | STATIC
        private String iface;    // ens192 등
        private Ipv4 ipv4;       // STATIC일 때만
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
