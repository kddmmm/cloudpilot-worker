package com.cloudrangers.cloudpilotworker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "provider.vsphere")
public class ProviderCredentials {

    private String server;
    private String user;
    private String password;
    private boolean allowUnverifiedSsl = true;

    // Default values
    private String datacenter = "Datacenter";
    private String cluster = "Cluster";
    private String datastore = "datastore1";
    private String template = "ubuntu-20.04-template";
    private String folder = "/vm";
    private String network = "VM Network";

    public String getServer() {
        return server;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isAllowUnverifiedSsl() {
        return allowUnverifiedSsl;
    }

    public void setAllowUnverifiedSsl(boolean allowUnverifiedSsl) {
        this.allowUnverifiedSsl = allowUnverifiedSsl;
    }

    public String getDatacenter() {
        return datacenter;
    }

    public void setDatacenter(String datacenter) {
        this.datacenter = datacenter;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getDatastore() {
        return datastore;
    }

    public void setDatastore(String datastore) {
        this.datastore = datastore;
    }

    public String getTemplate() {
        return template;
    }

    public void setTemplate(String template) {
        this.template = template;
    }

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public String getNetwork() {
        return network;
    }

    public void setNetwork(String network) {
        this.network = network;
    }
}