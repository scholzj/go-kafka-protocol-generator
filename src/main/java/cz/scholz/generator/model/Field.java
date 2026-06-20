package cz.scholz.generator.model;

import com.google.gson.annotations.SerializedName;
import java.util.ArrayList;
import java.util.List;

public class Field {
    private String name;
    private String type;
    private String versions;
    
    @SerializedName("nullableVersions")
    private String nullableVersions;
    
    @SerializedName("taggedVersions")
    private String taggedVersions;
    
    private Integer tag;
    private String about;
    private String defaultValue;
    private Boolean ignorable;
    
    @SerializedName("mapKey")
    private Boolean mapKey;
    
    @SerializedName("entityType")
    private String entityType;
    
    private List<Field> fields;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getVersions() {
        return versions;
    }

    public void setVersions(String versions) {
        this.versions = versions;
    }

    public String getNullableVersions() {
        return nullableVersions;
    }

    public void setNullableVersions(String nullableVersions) {
        this.nullableVersions = nullableVersions;
    }

    public String getTaggedVersions() {
        return taggedVersions;
    }

    public void setTaggedVersions(String taggedVersions) {
        this.taggedVersions = taggedVersions;
    }

    public Integer getTag() {
        return tag;
    }

    public void setTag(Integer tag) {
        this.tag = tag;
    }

    public String getAbout() {
        return about;
    }

    public void setAbout(String about) {
        this.about = about;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public Boolean getIgnorable() {
        return ignorable != null && ignorable;
    }

    public void setIgnorable(Boolean ignorable) {
        this.ignorable = ignorable;
    }

    public Boolean getMapKey() {
        return mapKey != null && mapKey;
    }

    public void setMapKey(Boolean mapKey) {
        this.mapKey = mapKey;
    }

    public String getEntityType() {
        return entityType;
    }

    public void setEntityType(String entityType) {
        this.entityType = entityType;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    /** Deep copy, used to inline a shared commonStruct independently at each reference site. */
    public Field copy() {
        Field c = new Field();
        c.name = name;
        c.type = type;
        c.versions = versions;
        c.nullableVersions = nullableVersions;
        c.taggedVersions = taggedVersions;
        c.tag = tag;
        c.about = about;
        c.defaultValue = defaultValue;
        c.ignorable = ignorable;
        c.mapKey = mapKey;
        c.entityType = entityType;
        if (fields != null) {
            c.fields = new ArrayList<>();
            for (Field f : fields) {
                c.fields.add(f.copy());
            }
        }
        return c;
    }
}

