package cz.scholz.generator.util;

import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.model.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Inlines a spec's {@code commonStructs} into the fields that reference them.
 *
 * <p>Kafka specs may declare shared struct definitions in a top-level {@code commonStructs} array
 * and reference them from a field by name (the field has the struct's name as its {@code type} but no
 * inline {@code fields}). The rest of the generator only understands inline nested structs, so this
 * resolver finds every such reference and attaches a deep copy of the common struct's fields to it.
 * Each reference gets its own copy, so the existing path-derived struct naming keeps every generated
 * type and method unique.
 */
public final class CommonStructResolver {

    private CommonStructResolver() {
    }

    public static void resolve(ApiSpec spec) {
        if (spec.getCommonStructs() == null || spec.getCommonStructs().isEmpty()) {
            return;
        }
        Map<String, Field> byName = new HashMap<>();
        for (Field cs : spec.getCommonStructs()) {
            byName.put(cs.getName(), cs);
        }
        resolveFields(spec.getFields(), byName);
    }

    private static void resolveFields(List<Field> fields, Map<String, Field> byName) {
        if (fields == null) {
            return;
        }
        for (Field field : fields) {
            // A reference to a common struct has the struct name as its (element) type and no inline
            // fields. Replace the missing fields with a fresh deep copy of the common struct's.
            if (field.getFields() == null || field.getFields().isEmpty()) {
                String elementType = TypeUtils.getArrayElementType(field.getType());
                Field commonStruct = byName.get(elementType);
                if (commonStruct != null && commonStruct.getFields() != null) {
                    field.setFields(commonStruct.copy().getFields());
                }
            }
            // Recurse: a (now inlined) struct may itself reference further common structs.
            resolveFields(field.getFields(), byName);
        }
    }
}
