package cz.scholz.generator.model;

import com.google.gson.annotations.SerializedName;
import java.util.List;

public class ApiSpec {
    @SerializedName("apiKey")
    private Integer apiKey;
    
    private String type;
    private String name;
    
    @SerializedName("validVersions")
    private String validVersions;
    
    @SerializedName("flexibleVersions")
    private String flexibleVersions;
    
    private List<String> listeners;
    private List<Field> fields;

    @SerializedName("commonStructs")
    private List<Field> commonStructs;

    public Integer getApiKey() {
        return apiKey;
    }

    public List<Field> getCommonStructs() {
        return commonStructs;
    }

    public void setApiKey(Integer apiKey) {
        this.apiKey = apiKey;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getValidVersions() {
        return validVersions;
    }

    public void setValidVersions(String validVersions) {
        this.validVersions = validVersions;
    }

    public String getFlexibleVersions() {
        return flexibleVersions;
    }

    public void setFlexibleVersions(String flexibleVersions) {
        this.flexibleVersions = flexibleVersions;
    }

    public List<String> getListeners() {
        return listeners;
    }

    public void setListeners(List<String> listeners) {
        this.listeners = listeners;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }
}

