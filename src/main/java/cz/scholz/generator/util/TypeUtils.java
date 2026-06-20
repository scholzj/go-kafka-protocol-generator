package cz.scholz.generator.util;

import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.model.Field;

import java.io.PrintWriter;
import java.util.List;

public class TypeUtils {
    /**
     * Generates the Go type for the given Kafka type.
     *
     * @param kafkaType     Kafka type as used in the JSON specifications
     *
     * @return  Go type which should be used for this field
     */
    public static String toGoType(String kafkaType) {
        if (isArrayType(kafkaType)) {
            return "*[]" + toNonArrayGoType(getArrayElementType(kafkaType), true);
        } else {
            return toNonArrayGoType(kafkaType, false);
        }
    }

    private static String toNonArrayGoType(String kafkaType, boolean isArrayType)    {
        switch (kafkaType) {
            case "int8":
                return "int8";
            case "int16":
                return "int16";
            case "int32":
                return "int32";
            case "int64":
                return "int64";
            case "bool":
                return "bool";
            case "string":
                return isArrayType ? "string" : "*string"; // Arrays use String directly. Otherwise, we always use a pointer.
            case "bytes":
            case "records":
                return "*[]byte";
            case "uuid":
                return "uuid.UUID";
            default:
                // Assume it's a custom type (struct)
                return isArrayType ? kafkaType : "*" + kafkaType; // Arrays use the type directly. Otherwise, we always use a pointer.
        }
    }

    public static boolean isArrayType(String type) {
        return type != null && type.startsWith("[]");
    }

    public static String getArrayElementType(String type) {
        if (isArrayType(type)) {
            return type.substring(2);
        }
        return type;
    }

    public static String getProtocolWriteMethod(String kafkaType, boolean nullable, boolean flexible, boolean isArray) {
        if (isArray) {
            if (flexible) {
                return "WriteNullableCompactArray";
            } else {
                if (nullable) {
                    return "WriteNullableArray";
                } else {
                    return "WriteArray";
                }
            }
        }
        
        switch (kafkaType) {
            case "int8":
                return "WriteInt8";
            case "int16":
                return "WriteInt16";
            case "int32":
                return "WriteInt32";
            case "int64":
                return "WriteInt64";
            case "bool":
                return "WriteBool";
            case "string":
                if (flexible) {
                    return nullable ? "WriteNullableCompactString" : "WriteCompactString";
                } else {
                    return nullable ? "WriteNullableString" : "WriteString";
                }
            case "bytes":
                if (flexible) {
                    return nullable ? "WriteNullableCompactBytes" : "WriteCompactBytes";
                } else {
                    return nullable ? "WriteNullableBytes" : "WriteBytes";
                }
            case "records":
                // Records are always nullable and use special methods
                return flexible ? "WriteCompactRecords" : "WriteRecords";
            case "uuid":
                return "WriteUUID";
            default:
                // Custom type - should use encoder method
                return null;
        }
    }

    public static String getProtocolReadMethod(String kafkaType, boolean nullable, boolean flexible, boolean isArray) {
        if (isArray) {
            if (flexible) {
                return "ReadNullableCompactArray";
            } else {
                if (nullable) {
                    return "ReadNullableArray";
                } else {
                    return "ReadArray";
                }
            }
        }
        
        switch (kafkaType) {
            case "int8":
                return "ReadInt8";
            case "int16":
                return "ReadInt16";
            case "int32":
                return "ReadInt32";
            case "int64":
                return "ReadInt64";
            case "bool":
                return "ReadBool";
            case "string":
                if (flexible) {
                    return nullable ? "ReadNullableCompactString" : "ReadCompactString";
                } else {
                    return nullable ? "ReadNullableString" : "ReadString";
                }
            case "bytes":
                if (flexible) {
                    return nullable ? "ReadNullableCompactBytes" : "ReadCompactBytes";
                } else {
                    return nullable ? "ReadNullableBytes" : "ReadBytes";
                }
            case "records":
                // Records are always nullable and use special methods
                return flexible ? "ReadCompactRecords" : "ReadRecords";
            case "uuid":
                return "ReadUUID";
            default:
                // Custom type - should use decoder method
                return null;
        }
    }

    public static String toCamelCase(String name) {
        if (name == null || name.isEmpty()) {
            return name;
        }
        return Character.toUpperCase(name.charAt(0)) + name.substring(1);
    }

    public static String toPackageName(String name) {
        // Convert ApiVersionsRequest -> apiversions
        return name
                .replace("Request", "")
                .replace("Response", "")
                .toLowerCase();
    }

    /**
     * The Go type name for a nested object: {@code Parent + SingularField}, e.g.
     * (MetadataResponseTopic, "Partitions") -> MetadataResponseTopicPartition.
     */
    public static String nestedStructName(String parentStruct, Field field) {
        String name = field.getName();
        if (name.endsWith("s")) {
            name = name.substring(0, name.length() - 1);
        }
        return parentStruct + toCamelCase(name);
    }

    public static boolean needsUUID(ApiSpec spec) {
        return hasFieldOfType("uuid", spec.getFields());
    }

    public static boolean hasFieldOfType(String type, List<Field> fields) {
        if (fields == null) return false;
        for (Field field : fields) {
            if (type.equals(field.getType()) || (field.getType() != null && field.getType().contains(type))) {
                return true;
            }
            if (hasFieldOfType(type, field.getFields())) {
                return true;
            }
        }
        return false;
    }

    public static boolean hasTaggedFieldsInNested(Field field) {
        if (field.getFields() == null) {
            return false;
        }
        return field.getFields().stream().anyMatch(f -> f.getTag() != null);
    }

    public static String about(Field field) {
        StringBuilder comment = new StringBuilder();

        comment.append("//");

        if (field.getAbout() != null && !field.getAbout().isEmpty()) {
            comment.append(" ").append(field.getAbout());
        }

        if (field.getTag() != null) {
            comment.append(" (tag: ").append(field.getTag()).append(")");
        }

        if (field.getVersions() != null) {
            comment.append(" (version: ").append(field.getVersions()).append(")");
        }

        if (field.getNullableVersions() != null) {
            comment.append(" (nullableVersion: ").append(field.getNullableVersions()).append(")");
        }

        return comment.toString();
    }
}

