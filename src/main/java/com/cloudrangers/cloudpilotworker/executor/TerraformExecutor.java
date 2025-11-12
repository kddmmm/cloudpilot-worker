package com.cloudrangers.cloudpilotworker.executor;

import com.cloudrangers.cloudpilotworker.config.ProviderCredentials;
import com.cloudrangers.cloudpilotworker.dto.ProvisionJobMessage;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class TerraformExecutor {

    @Value("${terraform.workspace:/tmp/terraform}")
    private String terraformWorkspace;

    @Value("${terraform.timeout-minutes:10}")
    private int timeoutMinutes;

    @Value("${terraform.mock-mode:false}")
    private boolean mockMode;

    private final ProviderCredentials providerCredentials;
    private final ObjectMapper objectMapper;

    public TerraformExecutor(ProviderCredentials providerCredentials, ObjectMapper objectMapper) {
        this.providerCredentials = providerCredentials;
        this.objectMapper = objectMapper;
    }

    public String execute(ProvisionJobMessage msg) throws Exception {
        if (mockMode) {
            log.info("(mock) terraform init/plan/apply - ws={}, jobId={}, vmName={}, cpu={}, mem={}GB, disk={}GB",
                    terraformWorkspace, msg.getJobId(), msg.getVmName(),
                    msg.getCpuCores(), msg.getMemoryGb(), msg.getDiskGb());
            Thread.sleep(800);
            return "vm-mock-" + msg.getVmName() + "-" + System.currentTimeMillis();
        }

        log.info("Starting Terraform execution for jobId={}, vmName={}", msg.getJobId(), msg.getVmName());
        Path jobWorkspace = Paths.get(terraformWorkspace, msg.getJobId());
        Files.createDirectories(jobWorkspace);

        try {
            createTerraformFiles(jobWorkspace, msg);
            executeCommand(jobWorkspace, "terraform", "init", "-input=false", "-no-color");
            executeCommand(jobWorkspace, "terraform", "plan", "-out=tfplan", "-input=false", "-no-color");
            executeCommand(jobWorkspace, "terraform", "apply", "-auto-approve", "tfplan");

            String vmId = extractFirstOutput(jobWorkspace, "vm_ids");
            if (vmId == null || vmId.isBlank()) {
                // fallback: 이름 반환
                vmId = extractFirstOutput(jobWorkspace, "vm_names");
            }
            if (vmId == null || vmId.isBlank()) {
                throw new RuntimeException("No VM id/name found in Terraform outputs");
            }

            log.info("Terraform execution completed successfully. jobId={}, vmId={}", msg.getJobId(), vmId);
            return vmId;

        } catch (Exception e) {
            log.error("Terraform execution failed for jobId={}: {}", msg.getJobId(), e.getMessage(), e);
            throw new RuntimeException("Terraform execution failed: " + e.getMessage(), e);
        }
    }

    private void createTerraformFiles(Path workspace, ProvisionJobMessage msg) throws IOException {
        // cloud-init user-data (Linux일 때만 생성)
        if (!isWindows(msg)) {
            String userData = renderCloudInit(msg);
            Files.writeString(workspace.resolve("user-data.yml"), userData, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        Files.writeString(workspace.resolve("main.tf"), generateMainTf(msg), StandardCharsets.UTF_8);
        Files.writeString(workspace.resolve("variables.tf"), generateVariablesTf(), StandardCharsets.UTF_8);
        Files.writeString(workspace.resolve("terraform.tfvars"), generateTfvars(msg), StandardCharsets.UTF_8);
        Files.writeString(workspace.resolve("outputs.tf"), generateOutputsTf(), StandardCharsets.UTF_8);
    }

    private boolean isWindows(ProvisionJobMessage msg) {
        return msg.getOs() != null
                && msg.getOs().getFamily() != null
                && msg.getOs().getFamily().toLowerCase(Locale.ROOT).contains("windows");
    }

    private String generateMainTf(ProvisionJobMessage msg) {
        boolean dhcp = msg.getNet() == null
                || msg.getNet().getMode() == null
                || msg.getNet().getMode().equalsIgnoreCase("DHCP");

        StringBuilder sb = new StringBuilder();

        sb.append("""
            terraform {
              required_providers {
                vsphere = {
                  source  = "hashicorp/vsphere"
                  version = "~> 2.5"
                }
              }
            }

            provider "vsphere" {
              user                 = var.vsphere_user
              password             = var.vsphere_password
              vsphere_server       = var.vsphere_server
              allow_unverified_ssl = var.allow_unverified_ssl
            }

            data "vsphere_datacenter" "dc" {
              name = var.datacenter
            }

            data "vsphere_compute_cluster" "cluster" {
              name          = var.cluster
              datacenter_id = data.vsphere_datacenter.dc.id
            }

            data "vsphere_datastore" "datastore" {
              name          = var.datastore
              datacenter_id = data.vsphere_datacenter.dc.id
            }

            data "vsphere_network" "network" {
              name          = var.network
              datacenter_id = data.vsphere_datacenter.dc.id
            }
            """);

        // 템플릿 UUID 직접 제공 시 우선 사용, 없으면 name/moid로 resolve
        sb.append("""
            locals {
              tpl_uuid = var.template_uuid != "" ? var.template_uuid : (
                var.template_moid != "" ? data.vsphere_virtual_machine.tpl_moid.id : data.vsphere_virtual_machine.tpl_name.id
              )
            }

            """);

        sb.append("""
            data "vsphere_virtual_machine" "tpl_name" {
              count         = var.template_uuid == "" && var.template_moid == "" && var.template_item_name != "" ? 1 : 0
              name          = var.template_item_name
              datacenter_id = data.vsphere_datacenter.dc.id
            }

            data "vsphere_virtual_machine" "tpl_moid" {
              count = var.template_uuid == "" && var.template_moid != "" ? 1 : 0
              moid  = var.template_moid
            }
            """);

        // VM 리소스
        sb.append("\nresource \"vsphere_virtual_machine\" \"vm\" {\n");
        sb.append("  count            = var.vm_count\n");
        sb.append("  name             = \"${var.vm_name}-${count.index + 1}\"\n");
        sb.append("  resource_pool_id = data.vsphere_compute_cluster.cluster.resource_pool_id\n");
        sb.append("  datastore_id     = data.vsphere_datastore.datastore.id\n");
        sb.append("  folder           = var.folder\n\n");

        sb.append("  num_cpus = var.cpu_cores\n");
        sb.append("  memory   = var.memory_gb * 1024\n");

        // guest_id 힌트가 있으면 반영(없어도 템플릿에서 자동)
        sb.append("  guest_id = var.guest_id != \"\" ? var.guest_id : null\n\n");

        sb.append("  network_interface {\n");
        sb.append("    network_id   = data.vsphere_network.network.id\n");
        sb.append("    adapter_type = \"vmxnet3\"\n");
        sb.append("  }\n\n");

        // 디스크 사이즈 증설(템플릿 보다 클 때만 의미 있음)
        sb.append("  disk {\n");
        sb.append("    label            = \"disk0\"\n");
        sb.append("    size             = var.disk_gb\n");
        sb.append("    thin_provisioned = var.thin_provisioned\n");
        sb.append("  }\n\n");

        // clone 블록
        sb.append("  clone {\n");
        sb.append("    template_uuid = local.tpl_uuid\n");

        if (isWindows(msg)) {
            sb.append("    customize {\n");
            sb.append("      windows_options {\n");
            sb.append("        computer_name  = var.hostname\n");
            sb.append("        time_zone      = var.win_time_zone\n");
            sb.append("        admin_password = var.win_admin_password != \"\" ? var.win_admin_password : null\n");
            sb.append("      }\n");
            if (!dhcp) {
                sb.append("      network_interface {\n");
                sb.append("        ipv4_address = var.ipv4_address\n");
                sb.append("        ipv4_netmask = var.ipv4_prefix\n");
                sb.append("      }\n");
                sb.append("      ipv4_gateway = var.ipv4_gateway\n");
                sb.append("      dns_server_list = var.dns_servers\n");
            }
            sb.append("    }\n");
        }
        else {
            // Linux는 DHCP면 네트워크 customize 생략
            if (!dhcp) {
                sb.append("    customize {\n");
                sb.append("      linux_options {\n");
                sb.append("        host_name = var.hostname\n");
                sb.append("        domain    = var.domain\n");
                sb.append("      }\n");
                sb.append("      network_interface {\n");
                sb.append("        ipv4_address = var.ipv4_address\n");
                sb.append("        ipv4_netmask = var.ipv4_prefix\n");
                sb.append("      }\n");
                sb.append("      ipv4_gateway = var.ipv4_gateway\n");
                sb.append("      dns_server_list = var.dns_servers\n");
                sb.append("    }\n");
            }
        }
        sb.append("  }\n\n");

        // cloud-init user-data를 guestinfo로 전달 (Linux일 때만)
        if (!isWindows(msg)) {
            sb.append("""
              extra_config = {
                "guestinfo.userdata"          = base64encode(file("user-data.yml"))
                "guestinfo.userdata.encoding" = "base64"
              }

              wait_for_guest_net_timeout = 10
              wait_for_guest_ip_timeout  = 10
              """);
        }

        sb.append("}\n");
        return sb.toString();
    }

    private String generateVariablesTf() {
        return """
            variable "vsphere_server" { type = string }
            variable "vsphere_user"   { type = string }
            variable "vsphere_password" { type = string, sensitive = true }
            variable "allow_unverified_ssl" { type = bool, default = true }

            variable "datacenter" { type = string }
            variable "cluster"    { type = string }
            variable "datastore"  { type = string }
            variable "network"    { type = string }
            variable "folder"     { type = string, default = "/vm" }

            variable "vm_name"   { type = string }
            variable "vm_count"  { type = number, default = 1 }
            variable "cpu_cores" { type = number }
            variable "memory_gb" { type = number }
            variable "disk_gb"   { type = number }
            variable "thin_provisioned" { type = bool, default = true }

            variable "template_uuid"      { type = string, default = "" }
            variable "template_item_name" { type = string, default = "" }
            variable "template_moid"      { type = string, default = "" }
            variable "guest_id"           { type = string, default = "" }

            # network/customize variables (STATIC일 때만 사용)
            variable "hostname"    { type = string, default = null }
            variable "domain"      { type = string, default = "local" }
            variable "ipv4_address"{ type = string, default = null }
            variable "ipv4_prefix" { type = number, default = null }
            variable "ipv4_gateway"{ type = string, default = null }
            variable "dns_servers" { type = list(string), default = [] }

            # windows 전용
            variable "win_admin_password" { type = string, default = "" }
            variable "win_time_zone"      { type = number, default = 225 } # Asia/Seoul

            """;
    }

    private String generateTfvars(ProvisionJobMessage msg) {
        String datacenter = pickStr(msg, "datacenter", providerCredentials.getDatacenter());
        String cluster    = pickStr(msg, "cluster", providerCredentials.getCluster());
        String datastore  = pickStr(msg, "datastore",
                msg.getTemplate() != null && msg.getTemplate().getTemplateDatastore() != null
                        ? msg.getTemplate().getTemplateDatastore()
                        : providerCredentials.getDatastore());
        String network    = pickStr(msg, "network", providerCredentials.getNetwork());
        String folder     = pickStr(msg, "folder", providerCredentials.getFolder() == null ? "/vm" : providerCredentials.getFolder());

        StringBuilder sb = new StringBuilder();
        sb.append("vsphere_server = \"").append(providerCredentials.getServer()).append("\"\n");
        sb.append("vsphere_user   = \"").append(providerCredentials.getUser()).append("\"\n");
        sb.append("vsphere_password = \"").append(providerCredentials.getPassword()).append("\"\n");
        sb.append("allow_unverified_ssl = true\n\n");

        sb.append("datacenter = \"").append(datacenter).append("\"\n");
        sb.append("cluster    = \"").append(cluster).append("\"\n");
        sb.append("datastore  = \"").append(datastore).append("\"\n");
        sb.append("network    = \"").append(network).append("\"\n");
        sb.append("folder     = \"").append(folder).append("\"\n\n");

        sb.append("vm_name   = \"").append(msg.getProperties() != null && msg.getProperties().getHostname() != null
                ? msg.getProperties().getHostname() : msg.getVmName()).append("\"\n");
        sb.append("vm_count  = ").append(msg.getVmCount() == null ? 1 : msg.getVmCount()).append("\n");
        sb.append("cpu_cores = ").append(msg.getCpuCores()).append("\n");
        sb.append("memory_gb = ").append(msg.getMemoryGb()).append("\n");
        sb.append("disk_gb   = ").append(msg.getDiskGb()).append("\n");
        sb.append("thin_provisioned = true\n\n");

        // template resolve
        String tplUuid = (msg.getTemplate() != null && msg.getTemplate().getUuid() != null) ? msg.getTemplate().getUuid() : "";
        String tplName = (msg.getTemplate() != null && msg.getTemplate().getItemName() != null) ? msg.getTemplate().getItemName() : "";
        String tplMoid = (msg.getTemplate() != null && msg.getTemplate().getTemplateMoid() != null) ? msg.getTemplate().getTemplateMoid() : "";
        String guestId = (msg.getTemplate() != null && msg.getTemplate().getGuestId() != null) ? msg.getTemplate().getGuestId() : "";

        sb.append("template_uuid      = \"").append(tplUuid).append("\"\n");
        sb.append("template_item_name = \"").append(tplName).append("\"\n");
        sb.append("template_moid      = \"").append(tplMoid).append("\"\n");
        sb.append("guest_id           = \"").append(guestId).append("\"\n\n");

        // network/customize vars (STATIC만)
        boolean dhcp = msg.getNet() == null || msg.getNet().getMode() == null || msg.getNet().getMode().equalsIgnoreCase("DHCP");
        sb.append("hostname = \"").append(msg.getProperties() != null && msg.getProperties().getHostname() != null
                ? msg.getProperties().getHostname() : msg.getVmName()).append("\"\n");
        if (!dhcp && msg.getNet().getIpv4() != null) {
            sb.append("ipv4_address = \"").append(nullSafe(msg.getNet().getIpv4().getAddress())).append("\"\n");
            Integer prefix = msg.getNet().getIpv4().getPrefix();
            sb.append("ipv4_prefix  = ").append(prefix == null ? "null" : prefix.toString()).append("\n");
            sb.append("ipv4_gateway = \"").append(nullSafe(msg.getNet().getIpv4().getGateway())).append("\"\n");
            if (msg.getNet().getDns() != null && !msg.getNet().getDns().isEmpty()) {
                sb.append("dns_servers  = [");
                for (int i = 0; i < msg.getNet().getDns().size(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append("\"").append(msg.getNet().getDns().get(i)).append("\"");
                }
                sb.append("]\n");
            }
        } else {
            sb.append("ipv4_address = null\nipv4_prefix = null\nipv4_gateway = null\n");
            sb.append("dns_servers = []\n");
        }
        sb.append("\n");

        // windows vars
        if (isWindows(msg) && msg.getProperties() != null && msg.getProperties().getWin() != null) {
            String pw = msg.getProperties().getWin().getAdmin_password();
            sb.append("win_admin_password = \"").append(pw == null ? "" : pw).append("\"\n");
        } else {
            sb.append("win_admin_password = \"\"\n");
        }

        return sb.toString();
    }

    private String generateOutputsTf() {
        return """
            output "vm_ids" {
              description = "IDs of created VMs"
              value       = vsphere_virtual_machine.vm[*].id
            }

            output "vm_names" {
              description = "Names of created VMs"
              value       = vsphere_virtual_machine.vm[*].name
            }

            output "vm_ip_addresses" {
              description = "IP addresses of created VMs"
              value       = vsphere_virtual_machine.vm[*].default_ip_address
            }
            """;
    }

    private String renderCloudInit(ProvisionJobMessage msg) {
        // 아주 기본적인 cloud-init user-data 생성 (DHCP 전제)
        String hostname = (msg.getProperties() != null && msg.getProperties().getHostname() != null)
                ? msg.getProperties().getHostname() : msg.getVmName();
        String timezone = (msg.getProperties() != null && msg.getProperties().getTimezone() != null)
                ? msg.getProperties().getTimezone() : "Asia/Seoul";

        StringBuilder y = new StringBuilder();
        y.append("#cloud-config\n");
        y.append("hostname: ").append(hostname).append("\n");
        y.append("timezone: ").append(timezone).append("\n");
        y.append("package_update: true\n");

        // users
        if (msg.getProperties() != null && msg.getProperties().getUsers() != null && !msg.getProperties().getUsers().isEmpty()) {
            y.append("users:\n");
            for (var u : msg.getProperties().getUsers()) {
                y.append("  - name: ").append(u.getName()).append("\n");
                if (u.getSudo() != null) y.append("    sudo: \"").append(u.getSudo()).append("\"\n");
                if (u.getShell() != null) y.append("    shell: ").append(u.getShell()).append("\n");
                if (u.getSsh_authorized_keys() != null && !u.getSsh_authorized_keys().isEmpty()) {
                    y.append("    ssh_authorized_keys:\n");
                    for (String k : u.getSsh_authorized_keys()) {
                        y.append("      - ").append(k).append("\n");
                    }
                }
            }
        }

        // packages
        if (msg.getProperties() != null && msg.getProperties().getPackages() != null && !msg.getProperties().getPackages().isEmpty()) {
            y.append("packages:\n");
            for (String p : msg.getProperties().getPackages()) {
                y.append("  - ").append(p).append("\n");
            }
        }

        // files
        if (msg.getProperties() != null && msg.getProperties().getFiles() != null && !msg.getProperties().getFiles().isEmpty()) {
            y.append("write_files:\n");
            for (var f : msg.getProperties().getFiles()) {
                y.append("  - path: ").append(f.getPath()).append("\n");
                if (f.getOwner() != null) y.append("    owner: ").append(f.getOwner()).append("\n");
                if (f.getPerm() != null)  y.append("    permissions: \"").append(f.getPerm()).append("\"\n");
                y.append("    content: |\n");
                for (String line : (f.getContent() == null ? "" : f.getContent()).split("\\R")) {
                    y.append("      ").append(line).append("\n");
                }
            }
        }

        // runcmd
        if (msg.getProperties() != null && msg.getProperties().getRuncmd() != null && !msg.getProperties().getRuncmd().isEmpty()) {
            y.append("runcmd:\n");
            for (String c : msg.getProperties().getRuncmd()) {
                y.append("  - ").append(c).append("\n");
            }
        }
        return y.toString();
    }

    private void executeCommand(Path workspace, String... command) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.directory(workspace.toFile());
        pb.environment().putAll(System.getenv());

        log.info("Executing: {} (dir: {})", String.join(" ", command), workspace);
        Process process = pb.start();

        Thread out = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = r.readLine()) != null) log.info("[{}] {}", command[0], line);
            } catch (IOException ignored) {}
        });
        Thread err = new Thread(() -> {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = r.readLine()) != null) log.error("[{}] {}", command[0], line);
            } catch (IOException ignored) {}
        });
        out.start(); err.start();

        boolean completed = process.waitFor(timeoutMinutes, TimeUnit.MINUTES);
        if (!completed) {
            process.destroyForcibly();
            throw new RuntimeException("Command timed out after " + timeoutMinutes + " minutes");
        }
        out.join(); err.join();

        if (process.exitValue() != 0) {
            throw new RuntimeException("Command failed: " + String.join(" ", command) + " (exit " + process.exitValue() + ")");
        }
    }

    private String extractFirstOutput(Path workspace, String key) throws Exception {
        ProcessBuilder pb = new ProcessBuilder("terraform", "output", "-json", key);
        pb.directory(workspace.toFile());

        Process process = pb.start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line; while ((line = reader.readLine()) != null) output.append(line);
        }
        process.waitFor();

        if (process.exitValue() != 0) return null;

        // terraform -json 값은 {"sensitive":false,"type":["list","string"],"value":[...]}
        Map<String, Object> node = objectMapper.readValue(output.toString(), new TypeReference<Map<String,Object>>(){});
        Object val = node.get("value");
        if (val instanceof List<?> list && !list.isEmpty()) {
            Object first = list.get(0);
            return first == null ? null : String.valueOf(first);
        }
        return null;
    }
    private String pickStr(ProvisionJobMessage msg, String key, String def) {
        if (msg.getAdditionalConfig() != null && msg.getAdditionalConfig().get(key) != null) {
            return String.valueOf(msg.getAdditionalConfig().get(key));
        }
        return def;
    }

    private String nullSafe(String s) { return s == null ? "" : s; }
}
