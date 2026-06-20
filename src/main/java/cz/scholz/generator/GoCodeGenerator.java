package cz.scholz.generator;

import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.model.Field;
import cz.scholz.generator.util.TypeUtils;
import cz.scholz.generator.util.VersionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates the Go encoding/decoding code for a single Kafka protocol message (one request or one
 * response described by a JSON spec file).
 *
 * <p>The generated code mirrors the hand-written style of github.com/scholzj/go-kafka-protocol. For
 * every message it emits, in order:
 * <ol>
 *   <li>the {@code package} declaration and imports,</li>
 *   <li>the main struct plus one struct per nested object (named {@code Parent + SingularField}),</li>
 *   <li>an {@code isRequestFlexible}/{@code isResponseFlexible} helper (flexible messages only),</li>
 *   <li>the {@code Write} and {@code Read} methods for the main struct,</li>
 *   <li>an {@code <field>Encoder}/{@code <field>Decoder} pair for every nested object, plus
 *       {@code taggedFieldsEncoder<Field>}/{@code taggedFieldsDecoder<Field>} when that object has
 *       its own tagged fields,</li>
 *   <li>the top level {@code taggedFieldsEncoder}/{@code taggedFieldsDecoder} (tagged messages only),</li>
 *   <li>{@code PrettyPrint} methods for the main and nested structs.</li>
 * </ol>
 *
 * <p>The output is intentionally verbose and heavily commented so it can double as protocol
 * documentation. It is emitted with tab indentation; running {@code gofmt} afterwards only adjusts
 * the alignment of trailing comments and the ordering of imports.
 *
 * <p>A few naming rules are worth keeping in mind while reading this class:
 * <ul>
 *   <li><b>Struct type names</b> are synthesised from the path through the message, not from the
 *       JSON type: the field {@code Partitions} inside {@code MetadataResponseTopic} becomes
 *       {@code MetadataResponseTopicPartition} (parent name + singularised field name).</li>
 *   <li><b>Encoder/decoder method names</b> come from the field name only ({@code partitionsEncoder})
 *       and are always methods on the <em>main</em> struct, so the receiver ({@code req}/{@code res})
 *       can reach {@code ApiVersion}.</li>
 *   <li>Inside encoders/decoders the nested value is addressed through the {@code value} parameter;
 *       in the top level {@code Write}/{@code Read} it is addressed through the receiver.</li>
 * </ul>
 */
public class GoCodeGenerator {
    private static final int NO_END = Integer.MAX_VALUE;

    private final ApiSpec spec;
    private final String packageName;
    private final String mainStruct;
    private final boolean isRequest;
    private final String receiver;          // "req" or "res"
    private final String paramType;         // "Request" or "Response"
    private final int flexibleVersionStart;
    private final boolean flexible;
    private final String flexibleFunc;      // isRequestFlexible / isResponseFlexible

    private final StringBuilder sb = new StringBuilder();

    /** Per struct-typed field: the unique base used for its encoder/decoder method names. */
    private final Map<Field, String> methodBaseNames = new IdentityHashMap<>();

    /** Go reserved words - a field whose lower-cased name hits one cannot be used as a local var. */
    private static final Set<String> GO_KEYWORDS = Set.of(
            "break", "case", "chan", "const", "continue", "default", "defer", "else", "fallthrough",
            "for", "func", "go", "goto", "if", "import", "interface", "map", "package", "range",
            "return", "select", "struct", "switch", "type", "var");

    public GoCodeGenerator(ApiSpec spec) {
        this.spec = spec;
        this.packageName = TypeUtils.toPackageName(spec.getName());
        this.mainStruct = spec.getName();
        this.isRequest = "request".equals(spec.getType());
        this.receiver = isRequest ? "req" : "res";
        this.paramType = isRequest ? "Request" : "Response";
        this.flexibleVersionStart = VersionUtils.getStartVersion(spec.getFlexibleVersions());
        this.flexible = flexibleVersionStart < NO_END;
        this.flexibleFunc = isRequest ? "isRequestFlexible" : "isResponseFlexible";
    }

    public String generate() {
        assignMethodBaseNames(mainStruct, spec.getFields(), new HashSet<>());
        generatePackageAndImports();
        generateStructs();
        if (flexible) {
            generateFlexibleFunc();
        }
        generateWrite();
        generateRead();
        generateEncodersAndDecoders(mainStruct, spec.getFields());
        if (hasTaggedFields(spec.getFields())) {
            generateTopLevelTaggedFieldsEncoder();
            generateTopLevelTaggedFieldsDecoder();
        }
        generatePrettyPrint();
        return sb.toString();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Low level emit helpers
    ////////////////////////////////////////////////////////////////////////////

    private void line(int indent, String text) {
        sb.append("\t".repeat(indent)).append(text).append("\n");
    }

    private void blank() {
        sb.append("\n");
    }

    ////////////////////////////////////////////////////////////////////////////
    // Package, imports and structs
    ////////////////////////////////////////////////////////////////////////////

    private void generatePackageAndImports() {
        line(0, "package " + packageName);
        blank();

        List<String> imports = new ArrayList<>();
        imports.add("\"bytes\"");
        imports.add("\"fmt\"");
        imports.add("\"io\"");
        imports.add("\"github.com/scholzj/go-kafka-protocol/protocol\"");
        if (TypeUtils.needsUUID(spec)) {
            imports.add("\"github.com/google/uuid\"");
        }

        line(0, "import (");
        for (String imp : imports) {
            line(1, imp);
        }
        line(0, ")");
        blank();
    }

    private void generateStructs() {
        generateStruct(mainStruct, spec.getFields(), true);
        generateNestedStructs(mainStruct, spec.getFields());
    }

    private void generateNestedStructs(String parentStruct, List<Field> fields) {
        for (Field field : fields) {
            if (hasFields(field)) {
                String nested = nestedStructName(parentStruct, field);
                generateStruct(nested, field.getFields(), false);
                generateNestedStructs(nested, field.getFields());
            }
        }
    }

    private void generateStruct(String structName, List<Field> fields, boolean main) {
        line(0, "type " + structName + " struct {");
        if (main) {
            line(1, "ApiVersion int16");
        }
        for (Field field : fields) {
            line(1, capitalize(field.getName()) + " " + goType(structName, field) + " " + structFieldComment(field));
        }
        line(1, "rawTaggedFields *[]protocol.TaggedField");
        line(0, "}");
        blank();
    }

    private String structFieldComment(Field field) {
        StringBuilder c = new StringBuilder("//");
        if (field.getTag() != null) {
            c.append(" tag ").append(field.getTag()).append(":");
        }
        if (field.getAbout() != null && !field.getAbout().isEmpty()) {
            c.append(" ").append(field.getAbout());
        }
        // Append the version metadata so the struct can double as protocol documentation.
        List<String> meta = new ArrayList<>();
        if (field.getVersions() != null) {
            meta.add("versions: " + field.getVersions());
        }
        if (field.getNullableVersions() != null) {
            meta.add("nullable: " + field.getNullableVersions());
        }
        if (!meta.isEmpty()) {
            c.append(" (").append(String.join(", ", meta)).append(")");
        }
        return c.toString();
    }

    private void generateFlexibleFunc() {
        line(0, "func " + flexibleFunc + "(apiVersion int16) bool {");
        line(1, "return apiVersion >= " + flexibleVersionStart);
        line(0, "}");
        blank();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Write
    ////////////////////////////////////////////////////////////////////////////

    private void generateWrite() {
        line(0, "func (" + receiver + " *" + mainStruct + ") Write(w io.Writer) error {");

        boolean hasTagged = false;
        for (Field field : spec.getFields()) {
            // Top level tagged fields are written by taggedFieldsEncoder(), never inline.
            if (field.getTag() != null) {
                hasTagged = true;
                continue;
            }
            fieldComment(field);
            writeFieldGuarded(field, mainStruct, receiver, 1);
            blank();
        }

        generateWriteTaggedFields(hasTagged, receiver, null);

        line(1, "return nil");
        line(0, "}");
        blank();
    }

    /** Emits the trailing tagged fields block of a Write/Encoder method. {@code nestedField} is null
     *  for the top-level Write and the owning struct field for a nested encoder. */
    private void generateWriteTaggedFields(boolean hasOwnTags, String accessor, Field nestedField) {
        if (!flexible) {
            return;
        }
        line(1, "// Tagged fields");
        line(1, "if " + flexibleCheck() + " {");
        if (hasOwnTags) {
            String encoder = nestedField == null
                    ? receiver + ".taggedFieldsEncoder()"
                    : receiver + ".taggedFieldsEncoder" + capitalize(methodBase(nestedField)) + "(value)";
            line(2, "taggedFields, err := " + encoder);
            line(2, "if err != nil {");
            line(3, "return err");
            line(2, "}");
            blank();
            line(2, "if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {");
            line(3, "return err");
            line(2, "}");
        } else {
            line(2, "rawTaggedFields := []protocol.TaggedField{}");
            line(2, "if " + accessor + ".rawTaggedFields != nil {");
            line(3, "rawTaggedFields = *" + accessor + ".rawTaggedFields");
            line(2, "}");
            line(2, "if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {");
            line(3, "return err");
            line(2, "}");
        }
        line(1, "}");
        blank();
    }

    /** Emits a field's version guard (if any) wrapping the actual write statements. */
    private void writeFieldGuarded(Field field, String parentStruct, String accessor, int indent) {
        String cond = versionCondition(field.getVersions());
        if (cond != null) {
            line(indent, "if " + cond + " {");
            writeFieldValue(field, parentStruct, accessor, indent + 1);
            line(indent, "}");
        } else {
            writeFieldValue(field, parentStruct, accessor, indent);
        }
    }

    private void writeFieldValue(Field field, String parentStruct, String accessor, int indent) {
        String type = field.getType();
        String ref = accessor + "." + capitalize(field.getName());

        // Encode-side validation: a pointer field must not be nil in versions where it is non-nullable.
        emitNullCheck(field, parentStruct, ref, indent);

        if (TypeUtils.isArrayType(type)) {
            String elem = TypeUtils.getArrayElementType(type);
            String elemEncoderCompact;
            String elemEncoderPlain;
            if (hasFields(field)) {
                String encoder = receiver + "." + methodName(field, "Encoder");
                elemEncoderCompact = encoder;
                elemEncoderPlain = encoder;
            } else {
                elemEncoderCompact = "protocol." + TypeUtils.getProtocolWriteMethod(elem, false, true, false);
                elemEncoderPlain = "protocol." + TypeUtils.getProtocolWriteMethod(elem, false, false, false);
            }
            writeArray(ref, elemEncoderCompact, elemEncoderPlain, field.getNullableVersions() != null, indent);
        } else if (hasFields(field)) {
            // Single nested struct - always a pointer, written through its encoder.
            line(indent, "if err := " + receiver + "." + methodName(field, "Encoder") + "(w, *" + ref + "); err != nil {");
            line(indent + 1, "return err");
            line(indent, "}");
        } else if (type.equals("string")) {
            writeLengthPrefixed(field, ref, indent, "String");
        } else if (type.equals("bytes")) {
            writeLengthPrefixed(field, ref, indent, "Bytes");
        } else if (type.equals("records")) {
            // Records use the compact length only in flexible versions; decide at runtime so that the
            // non-flexible versions of an otherwise-flexible message encode them correctly.
            if (flexible) {
                line(indent, "if " + flexibleCheck() + " {");
                writeSimple("protocol.WriteCompactRecords(w, " + ref + ")", indent + 1);
                line(indent, "} else {");
                writeSimple("protocol.WriteRecords(w, " + ref + ")", indent + 1);
                line(indent, "}");
            } else {
                writeSimple("protocol.WriteRecords(w, " + ref + ")", indent);
            }
        } else {
            // int8/int16/int32/int64/bool/uuid - written by value.
            String method = TypeUtils.getProtocolWriteMethod(type, false, false, false);
            writeSimple("protocol." + method + "(w, " + ref + ")", indent);
        }
    }

    private void writeArray(String ref, String elemEncoderCompact, String elemEncoderPlain, boolean nullable, int indent) {
        // The compact (flexible) form is nullable by nature. The plain form passes the pointer for a
        // nullable array (so nil can be encoded) and the dereferenced slice otherwise.
        String plain = nullable
                ? "protocol.WriteNullableArray(w, " + elemEncoderPlain + ", " + ref + ")"
                : "protocol.WriteArray(w, " + elemEncoderPlain + ", *" + ref + ")";
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            writeSimple("protocol.WriteNullableCompactArray(w, " + elemEncoderCompact + ", " + ref + ")", indent + 1);
            line(indent, "} else {");
            writeSimple(plain, indent + 1);
            line(indent, "}");
        } else {
            writeSimple(plain, indent);
        }
    }

    /**
     * Emits the write for a length-prefixed pointer type ({@code word} is "String" or "Bytes").
     * A nullable field passes the pointer to the {@code WriteNullable*} helper (so nil is encoded);
     * a non-nullable field dereferences it. Flexible versions use the compact helper.
     */
    private void writeLengthPrefixed(Field field, String ref, int indent, String word) {
        boolean nullable = field.getNullableVersions() != null;
        String arg = nullable ? ref : "*" + ref;
        String compact = "protocol." + (nullable ? "WriteNullableCompact" : "WriteCompact") + word + "(w, " + arg + ")";
        String plain = "protocol." + (nullable ? "WriteNullable" : "Write") + word + "(w, " + arg + ")";
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            writeSimple(compact, indent + 1);
            line(indent, "} else {");
            writeSimple(plain, indent + 1);
            line(indent, "}");
        } else {
            writeSimple(plain, indent);
        }
    }

    /** Emits {@code if err := <call>; err != nil { return err }}. */
    private void writeSimple(String call, int indent) {
        line(indent, "if err := " + call + "; err != nil {");
        line(indent + 1, "return err");
        line(indent, "}");
    }

    /**
     * Emits an encode-side guard rejecting a nil value for a pointer field in versions where the
     * field is non-nullable. Called from inside the field's presence guard, so it only needs the
     * "non-nullable" part of the condition. No-op for value types and for fields that are nullable
     * across their whole life.
     */
    private void emitNullCheck(Field field, String parentStruct, String ref, int indent) {
        String predicate = nonNullableVersionPredicate(field);
        if (predicate == null) {
            return;
        }
        String cond = predicate.isEmpty() ? ref + " == nil" : predicate + " && " + ref + " == nil";
        line(indent, "if " + cond + " {");
        line(indent + 1, "return fmt.Errorf(\"" + parentStruct + "." + capitalize(field.getName())
                + " must not be nil in version %d\", " + receiver + ".ApiVersion)");
        line(indent, "}");
    }

    /**
     * Returns the version predicate (using the receiver's ApiVersion) selecting the versions in
     * which a present pointer field is non-nullable, or {@code null} when no null check is needed:
     * <ul>
     *   <li>{@code null} - the field is a value type, or nullable across its whole presence range;</li>
     *   <li>{@code ""}   - non-nullable in every version it is present (check applies unconditionally);</li>
     *   <li>a Go boolean expression - non-nullable only in the versions it matches.</li>
     * </ul>
     */
    private String nonNullableVersionPredicate(Field field) {
        if (!isPointerType(field)) {
            return null; // value types (ints, bool, uuid) can never be nil
        }
        if (field.getNullableVersions() == null) {
            return ""; // never nullable -> must always be present when written
        }

        int pStart = VersionUtils.getStartVersion(field.getVersions());
        int pEnd = VersionUtils.getEndVersion(field.getVersions());
        int nStart = VersionUtils.getStartVersion(field.getNullableVersions());
        int nEnd = VersionUtils.getEndVersion(field.getNullableVersions());

        if (nStart <= pStart && pEnd <= nEnd) {
            return null; // nullable everywhere the field exists
        }
        if (pEnd < nStart || pStart > nEnd) {
            return ""; // the nullable range never overlaps the field's versions
        }

        // Partial overlap: non-nullable below the nullable range and/or above it.
        List<String> parts = new ArrayList<>();
        if (nStart > pStart) {
            parts.add(receiver + ".ApiVersion < " + nStart);
        }
        if (nEnd < pEnd) {
            parts.add(receiver + ".ApiVersion > " + nEnd);
        }
        return parts.size() == 1 ? parts.get(0) : "(" + String.join(" || ", parts) + ")";
    }

    ////////////////////////////////////////////////////////////////////////////
    // Read
    ////////////////////////////////////////////////////////////////////////////

    private void generateRead() {
        String param = paramType.toLowerCase();
        line(0, "// TODO: pass version and bytes only");
        line(0, "func (" + receiver + " *" + mainStruct + ") Read(" + param + " *protocol." + paramType + ") error {");
        line(1, "if " + param + " == nil || " + param + ".Body == nil {");
        line(2, "return fmt.Errorf(\"" + mainStruct + ".Read: " + param + " or its body is nil\")");
        line(1, "}");
        blank();
        line(1, "r := bytes.NewBuffer(" + param + ".Body.Bytes())");
        line(1, receiver + ".ApiVersion = " + param + ".ApiVersion");
        blank();

        boolean hasTagged = false;
        for (Field field : spec.getFields()) {
            if (field.getTag() != null) {
                hasTagged = true;
                continue;
            }
            fieldComment(field);
            readFieldGuarded(field, mainStruct, receiver, "return err", 1);
            blank();
        }

        generateReadTaggedFields(hasTagged, receiver, "return err", null);

        line(1, "return nil");
        line(0, "}");
        blank();
    }

    /** Emits the trailing tagged fields block of a Read/Decoder method. {@code nestedField} is null
     *  for the top-level Read and the owning struct field for a nested decoder. */
    private void generateReadTaggedFields(boolean hasOwnTags, String accessor, String errReturn, Field nestedField) {
        if (!flexible) {
            return;
        }
        line(1, "// Tagged fields");
        line(1, "if " + flexibleCheck() + " {");
        if (hasOwnTags) {
            if (nestedField == null) {
                line(2, "if err := protocol.ReadTaggedFields(r, " + receiver + ".taggedFieldsDecoder); err != nil {");
                line(3, "return err");
                line(2, "}");
            } else {
                line(2, "if err := protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {");
                line(3, "return " + receiver + ".taggedFieldsDecoder" + capitalize(methodBase(nestedField)) + "(r, tag, tagLength, &" + accessor + ")");
                line(2, "}); err != nil {");
                line(3, errReturn);
                line(2, "}");
            }
        } else {
            line(2, "rawTaggedFields, err := protocol.ReadRawTaggedFields(r)");
            line(2, "if err != nil {");
            line(3, errReturn);
            line(2, "}");
            line(2, accessor + ".rawTaggedFields = &rawTaggedFields");
        }
        line(1, "}");
        blank();
    }

    private void readFieldGuarded(Field field, String parentStruct, String target, String errReturn, int indent) {
        String cond = versionCondition(field.getVersions());
        if (cond != null) {
            line(indent, "if " + cond + " {");
            readFieldValue(field, parentStruct, target, errReturn, indent + 1);
            line(indent, "}");
        } else {
            readFieldValue(field, parentStruct, target, errReturn, indent);
        }
    }

    private void readFieldValue(Field field, String parentStruct, String target, String errReturn, int indent) {
        String type = field.getType();
        String var = safeVar(field.getName());
        String dst = target + "." + capitalize(field.getName());

        if (TypeUtils.isArrayType(type)) {
            String elem = TypeUtils.getArrayElementType(type);
            String decoderCompact;
            String decoderPlain;
            if (hasFields(field)) {
                String decoder = receiver + "." + methodName(field, "Decoder");
                decoderCompact = decoder;
                decoderPlain = decoder;
            } else {
                decoderCompact = "protocol." + TypeUtils.getProtocolReadMethod(elem, false, true, false);
                decoderPlain = "protocol." + TypeUtils.getProtocolReadMethod(elem, false, false, false);
            }
            readArray(var, dst, decoderCompact, decoderPlain, field.getNullableVersions() != null, errReturn, indent);
        } else if (hasFields(field)) {
            // Single nested struct.
            line(indent, var + ", err := " + receiver + "." + methodName(field, "Decoder") + "(r)");
            readErrCheck(errReturn, indent);
            line(indent, dst + " = &" + var);
        } else if (type.equals("string")) {
            readLengthPrefixed(field, var, dst, errReturn, indent, "String");
        } else if (type.equals("bytes")) {
            readLengthPrefixed(field, var, dst, errReturn, indent, "Bytes");
        } else if (type.equals("records")) {
            // Records use the compact length only in flexible versions (decided at runtime).
            if (flexible) {
                line(indent, "if " + flexibleCheck() + " {");
                line(indent + 1, var + ", err := protocol.ReadCompactRecords(r)");
                readErrCheck(errReturn, indent + 1);
                line(indent + 1, dst + " = " + var);
                line(indent, "} else {");
                line(indent + 1, var + ", err := protocol.ReadRecords(r)");
                readErrCheck(errReturn, indent + 1);
                line(indent + 1, dst + " = " + var);
                line(indent, "}");
            } else {
                line(indent, var + ", err := protocol.ReadRecords(r)");
                readErrCheck(errReturn, indent);
                line(indent, dst + " = " + var); // records read returns *[]byte
            }
        } else {
            String method = TypeUtils.getProtocolReadMethod(type, false, false, false);
            line(indent, var + ", err := protocol." + method + "(r)");
            readErrCheck(errReturn, indent);
            line(indent, dst + " = " + var);
        }
    }

    private void readArray(String var, String dst, String decoderCompact, String decoderPlain, boolean nullable, String errReturn, int indent) {
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            line(indent + 1, var + ", err := protocol.ReadNullableCompactArray(r, " + decoderCompact + ")");
            readErrCheck(errReturn, indent + 1);
            line(indent + 1, dst + " = " + var); // ReadNullableCompactArray returns *[]T
            line(indent, "} else {");
            readPlainArray(var, dst, decoderPlain, nullable, errReturn, indent + 1);
            line(indent, "}");
        } else {
            readPlainArray(var, dst, decoderPlain, nullable, errReturn, indent);
        }
    }

    private void readPlainArray(String var, String dst, String decoderPlain, boolean nullable, String errReturn, int indent) {
        // ReadNullableArray returns *[]T (assignable directly); ReadArray returns []T (we address it).
        String reader = nullable ? "ReadNullableArray" : "ReadArray";
        line(indent, var + ", err := protocol." + reader + "(r, " + decoderPlain + ")");
        readErrCheck(errReturn, indent);
        line(indent, nullable ? dst + " = " + var : dst + " = &" + var);
    }

    /** Reads a length-prefixed pointer type ({@code word} is "String" or "Bytes"), mirroring
     *  {@link #writeLengthPrefixed}. */
    private void readLengthPrefixed(Field field, String var, String dst, String errReturn, int indent, String word) {
        boolean nullable = field.getNullableVersions() != null;
        // Nullable reads return a pointer already; non-nullable reads return a value we address.
        String assign = nullable ? dst + " = " + var : dst + " = &" + var;
        String compact = (nullable ? "ReadNullableCompact" : "ReadCompact") + word;
        String plain = (nullable ? "ReadNullable" : "Read") + word;
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            line(indent + 1, var + ", err := protocol." + compact + "(r)");
            readErrCheck(errReturn, indent + 1);
            line(indent + 1, assign);
            line(indent, "} else {");
            line(indent + 1, var + ", err := protocol." + plain + "(r)");
            readErrCheck(errReturn, indent + 1);
            line(indent + 1, assign);
            line(indent, "}");
        } else {
            line(indent, var + ", err := protocol." + plain + "(r)");
            readErrCheck(errReturn, indent);
            line(indent, assign);
        }
    }

    private void readErrCheck(String errReturn, int indent) {
        line(indent, "if err != nil {");
        line(indent + 1, errReturn);
        line(indent, "}");
    }

    ////////////////////////////////////////////////////////////////////////////
    // Nested encoders / decoders
    ////////////////////////////////////////////////////////////////////////////

    private void generateEncodersAndDecoders(String parentStruct, List<Field> fields) {
        for (Field field : fields) {
            if (!hasFields(field)) {
                continue;
            }
            String nested = nestedStructName(parentStruct, field);
            generateEncoder(field, nested);
            generateDecoder(field, nested);
            if (flexible && hasTaggedFields(field.getFields())) {
                generateNestedTaggedFieldsEncoder(field, nested);
                generateNestedTaggedFieldsDecoder(field, nested);
            }
            generateEncodersAndDecoders(nested, field.getFields());
        }
    }

    private void generateEncoder(Field field, String nested) {
        line(0, "func (" + receiver + " *" + mainStruct + ") " + methodName(field, "Encoder") + "(w io.Writer, value " + nested + ") error {");
        for (Field sub : field.getFields()) {
            emitNestedFieldWrite(sub, nested);
        }
        generateWriteTaggedFields(hasTaggedFields(field.getFields()), "value", field);
        line(1, "return nil");
        line(0, "}");
        blank();
    }

    private void emitNestedFieldWrite(Field sub, String nested) {
        fieldComment(sub);
        if (flexible && sub.getTag() != null) {
            // In flexible versions a tagged field is serialised by the tagged fields encoder. It is
            // only written inline for the older, non-flexible versions in which it is a normal field.
            line(1, "if !" + flexibleCheck() + " {");
            writeFieldGuarded(sub, nested, "value", 2);
            line(1, "}");
        } else {
            writeFieldGuarded(sub, nested, "value", 1);
        }
        blank();
    }

    private void generateDecoder(Field field, String nested) {
        String var = nested.toLowerCase();
        line(0, "func (" + receiver + " *" + mainStruct + ") " + methodName(field, "Decoder") + "(r io.Reader) (" + nested + ", error) {");
        line(1, var + " := " + nested + "{}");
        blank();
        String errReturn = "return " + var + ", err";
        for (Field sub : field.getFields()) {
            emitNestedFieldRead(sub, nested, var, errReturn);
        }
        generateReadTaggedFields(hasTaggedFields(field.getFields()), var, errReturn, field);
        line(1, "return " + var + ", nil");
        line(0, "}");
        blank();
    }

    private void emitNestedFieldRead(Field sub, String nested, String var, String errReturn) {
        fieldComment(sub);
        if (flexible && sub.getTag() != null) {
            line(1, "if !" + flexibleCheck() + " {");
            readFieldGuarded(sub, nested, var, errReturn, 2);
            line(1, "}");
        } else {
            readFieldGuarded(sub, nested, var, errReturn, 1);
        }
        blank();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Tagged fields encoders / decoders
    ////////////////////////////////////////////////////////////////////////////

    private void generateTopLevelTaggedFieldsEncoder() {
        List<Field> tagged = taggedFields(spec.getFields());
        line(0, "func (" + receiver + " *" + mainStruct + ") taggedFieldsEncoder() ([]protocol.TaggedField, error) {");
        emitTaggedFieldsEncoderBody(tagged, receiver);
        line(0, "}");
        blank();
    }

    private void generateNestedTaggedFieldsEncoder(Field field, String nested) {
        List<Field> tagged = taggedFields(field.getFields());
        line(0, "func (" + receiver + " *" + mainStruct + ") taggedFieldsEncoder" + capitalize(methodBase(field)) + "(value " + nested + ") ([]protocol.TaggedField, error) {");
        emitTaggedFieldsEncoderBody(tagged, "value");
        line(0, "}");
        blank();
    }

    private void emitTaggedFieldsEncoderBody(List<Field> tagged, String accessor) {
        line(1, "rawTaggedFieldsLen := 0");
        line(1, "if " + accessor + ".rawTaggedFields != nil {");
        line(2, "rawTaggedFieldsLen = len(*" + accessor + ".rawTaggedFields)");
        line(1, "}");
        line(1, "taggedFields := make([]protocol.TaggedField, 0, " + tagged.size() + "+rawTaggedFieldsLen)");
        blank();
        line(1, "buf := bytes.NewBuffer(make([]byte, 0))");
        blank();

        for (Field field : tagged) {
            emitTaggedFieldWrite(field, accessor);
        }

        line(1, "// We append any raw tagged fields to the end of the array");
        line(1, "if " + accessor + ".rawTaggedFields != nil {");
        line(2, "taggedFields = append(taggedFields, *" + accessor + ".rawTaggedFields...)");
        line(1, "}");
        blank();
        line(1, "return taggedFields, nil");
    }

    private void emitTaggedFieldWrite(Field field, String accessor) {
        String ref = accessor + "." + capitalize(field.getName());
        line(1, "// Tag " + field.getTag());
        if (isPointerType(field)) {
            // Pointer-typed tagged fields are only serialised when present.
            line(1, "if " + ref + " != nil {");
            line(2, "buf = bytes.NewBuffer(make([]byte, 0))");
            emitTaggedValueWrite(field, ref, 2);
            blank();
            line(2, "taggedFields = append(taggedFields, protocol.TaggedField{Tag: " + field.getTag() + ", Field: buf.Bytes()})");
            line(1, "}");
        } else {
            line(1, "buf = bytes.NewBuffer(make([]byte, 0))");
            emitTaggedValueWrite(field, ref, 1);
            blank();
            line(1, "taggedFields = append(taggedFields, protocol.TaggedField{Tag: " + field.getTag() + ", Field: buf.Bytes()})");
        }
        blank();
    }

    /** Writes one tagged field into {@code buf}. Tagged fields only exist in flexible versions, so
     *  the compact encodings are always used. */
    private void emitTaggedValueWrite(Field field, String ref, int indent) {
        String type = field.getType();
        String call;
        if (TypeUtils.isArrayType(type)) {
            String encoder = hasFields(field)
                    ? receiver + "." + methodName(field, "Encoder")
                    : "protocol." + TypeUtils.getProtocolWriteMethod(TypeUtils.getArrayElementType(type), false, true, false);
            call = "protocol.WriteNullableCompactArray(buf, " + encoder + ", " + ref + ")";
        } else if (hasFields(field)) {
            call = receiver + "." + methodName(field, "Encoder") + "(buf, *" + ref + ")";
        } else if (type.equals("string") || type.equals("bytes")) {
            String word = type.equals("string") ? "String" : "Bytes";
            boolean nullable = field.getNullableVersions() != null;
            call = nullable
                    ? "protocol.WriteNullableCompact" + word + "(buf, " + ref + ")"
                    : "protocol.WriteCompact" + word + "(buf, *" + ref + ")";
        } else if (type.equals("records")) {
            call = "protocol.WriteCompactRecords(buf, " + ref + ")";
        } else {
            call = "protocol." + TypeUtils.getProtocolWriteMethod(type, false, true, false) + "(buf, " + ref + ")";
        }
        line(indent, "if err := " + call + "; err != nil {");
        line(indent + 1, "return taggedFields, err");
        line(indent, "}");
    }

    private void generateTopLevelTaggedFieldsDecoder() {
        List<Field> tagged = taggedFields(spec.getFields());
        line(0, "func (" + receiver + " *" + mainStruct + ") taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {");
        emitTaggedFieldsDecoderBody(tagged, receiver);
        line(0, "}");
        blank();
    }

    private void generateNestedTaggedFieldsDecoder(Field field, String nested) {
        List<Field> tagged = taggedFields(field.getFields());
        line(0, "func (" + receiver + " *" + mainStruct + ") taggedFieldsDecoder" + capitalize(methodBase(field)) + "(r io.Reader, tag uint64, tagLength uint64, value *" + nested + ") error {");
        emitTaggedFieldsDecoderBody(tagged, "value");
        line(0, "}");
        blank();
    }

    private void emitTaggedFieldsDecoderBody(List<Field> tagged, String accessor) {
        line(1, "rawTaggedFields := make([]protocol.TaggedField, 0)");
        blank();
        line(1, "switch tag {");
        for (Field field : tagged) {
            emitTaggedFieldRead(field, accessor);
        }
        line(1, "default:");
        line(2, "// Unknown tag - keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)");
        line(2, "field, err := io.ReadAll(r)");
        line(2, "if err != nil {");
        line(3, "return err");
        line(2, "}");
        line(2, "rawTaggedFields = append(rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})");
        line(1, "}");
        blank();
        line(1, "// Set the raw tagged fields");
        line(1, accessor + ".rawTaggedFields = &rawTaggedFields");
        blank();
        line(1, "return nil");
    }

    private void emitTaggedFieldRead(Field field, String accessor) {
        String type = field.getType();
        String var = safeVar(field.getName());
        String dst = accessor + "." + capitalize(field.getName());
        line(1, "case " + field.getTag() + ":");
        line(2, "// " + capitalize(field.getName()));

        if (TypeUtils.isArrayType(type)) {
            String decoder = hasFields(field)
                    ? receiver + "." + methodName(field, "Decoder")
                    : "protocol." + TypeUtils.getProtocolReadMethod(TypeUtils.getArrayElementType(type), false, true, false);
            line(2, var + ", err := protocol.ReadNullableCompactArray(r, " + decoder + ")");
            readErrCheck("return err", 2);
            line(2, dst + " = " + var);
        } else if (hasFields(field)) {
            line(2, var + "Val, err := " + receiver + "." + methodName(field, "Decoder") + "(r)");
            readErrCheck("return err", 2);
            line(2, dst + " = &" + var + "Val");
        } else if (type.equals("string") || type.equals("bytes")) {
            String word = type.equals("string") ? "String" : "Bytes";
            boolean nullable = field.getNullableVersions() != null;
            String method = (nullable ? "ReadNullableCompact" : "ReadCompact") + word;
            line(2, var + ", err := protocol." + method + "(r)");
            readErrCheck("return err", 2);
            line(2, nullable ? dst + " = " + var : dst + " = &" + var);
        } else if (type.equals("records")) {
            line(2, var + ", err := protocol.ReadCompactRecords(r)");
            readErrCheck("return err", 2);
            line(2, dst + " = " + var);
        } else {
            String method = TypeUtils.getProtocolReadMethod(type, false, true, false);
            line(2, var + ", err := protocol." + method + "(r)");
            readErrCheck("return err", 2);
            line(2, dst + " = " + var);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // PrettyPrint
    ////////////////////////////////////////////////////////////////////////////

    private void generatePrettyPrint() {
        line(0, "//goland:noinspection GoUnhandledErrorResult");
        line(0, "func (" + receiver + " *" + mainStruct + ") PrettyPrint() string {");
        line(1, "w := bytes.NewBuffer([]byte{})");
        blank();
        String arrow = isRequest ? "->" : "<-";
        line(1, "fmt.Fprintf(w, \"" + spaces(1) + arrow + " " + mainStruct + ":\\n\")");
        for (Field field : spec.getFields()) {
            prettyField(field, receiver, 1);
        }
        blank();
        line(1, "return w.String()");
        line(0, "}");
        blank();

        generateNestedPrettyPrint(mainStruct, spec.getFields(), 2);
    }

    private void generateNestedPrettyPrint(String parentStruct, List<Field> fields, int level) {
        for (Field field : fields) {
            if (!hasFields(field)) {
                continue;
            }
            String nested = nestedStructName(parentStruct, field);
            line(0, "//goland:noinspection GoUnhandledErrorResult");
            line(0, "func (value *" + nested + ") PrettyPrint() string {");
            line(1, "w := bytes.NewBuffer([]byte{})");
            blank();
            for (Field sub : field.getFields()) {
                prettyField(sub, "value", level);
            }
            blank();
            line(1, "return w.String()");
            line(0, "}");
            blank();

            generateNestedPrettyPrint(nested, field.getFields(), level + 1);
        }
    }

    /**
     * Emits the PrettyPrint statements for one field. {@code level} is the nesting depth of the
     * struct that owns the field (1 for the main struct); its fields are printed indented one level
     * deeper.
     */
    private void prettyField(Field field, String accessor, int level) {
        String name = capitalize(field.getName());
        String ref = accessor + "." + name;
        String loopVar = safeVar(field.getName());
        String fieldIndent = spaces(level + 1);
        String elementSeparatorIndent = spaces(level + 2);

        if (TypeUtils.isArrayType(field.getType())) {
            if (hasFields(field)) {
                // Blank lines around the block keep the generated PrettyPrint readable.
                // (gofmt collapses any resulting double blanks down to one.)
                blank();
                line(1, "if " + ref + " != nil {");
                line(2, "fmt.Fprintf(w, \"" + fieldIndent + name + ":\\n\")");
                line(2, "for _, " + loopVar + " := range *" + ref + " {");
                line(3, "fmt.Fprintf(w, \"%s\", " + loopVar + ".PrettyPrint())");
                line(3, "fmt.Fprintf(w, \"" + elementSeparatorIndent + "----------------\\n\")");
                line(2, "}");
                line(1, "} else {");
                line(2, "fmt.Fprintf(w, \"" + fieldIndent + name + ": nil\\n\")");
                line(1, "}");
                blank();
            } else {
                prettyNullablePrimitive(ref, fieldIndent, name);
            }
        } else if (hasFields(field)) {
            blank();
            line(1, "fmt.Fprintf(w, \"" + fieldIndent + name + ":\\n\")");
            line(1, "if " + ref + " != nil {");
            line(2, "fmt.Fprintf(w, \"%s\", " + ref + ".PrettyPrint())");
            line(1, "} else {");
            line(2, "fmt.Fprintf(w, \"" + spaces(level + 2) + "nil\\n\")");
            line(1, "}");
            blank();
        } else if (field.getType().equals("records") || field.getType().equals("bytes")) {
            // Raw byte payloads are not human-readable, so just print the length as a placeholder.
            blank();
            line(1, "if " + ref + " != nil {");
            line(2, "fmt.Fprintf(w, \"" + fieldIndent + name + ": <%d bytes>\\n\", len(*" + ref + "))");
            line(1, "} else {");
            line(2, "fmt.Fprintf(w, \"" + fieldIndent + name + ": nil\\n\")");
            line(1, "}");
            blank();
        } else if (isPointerType(field)) {
            prettyNullablePrimitive(ref, fieldIndent, name);
        } else {
            line(1, "fmt.Fprintf(w, \"" + fieldIndent + name + ": %v\\n\", " + ref + ")");
        }
    }

    private void prettyNullablePrimitive(String ref, String fieldIndent, String name) {
        blank();
        line(1, "if " + ref + " != nil {");
        line(2, "fmt.Fprintf(w, \"" + fieldIndent + name + ": %v\\n\", *" + ref + ")");
        line(1, "} else {");
        line(2, "fmt.Fprintf(w, \"" + fieldIndent + name + ": nil\\n\")");
        line(1, "}");
        blank();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Shared helpers
    ////////////////////////////////////////////////////////////////////////////

    private void fieldComment(Field field) {
        line(1, "// " + capitalize(field.getName()) + " (versions: " + field.getVersions() + ")");
    }

    private String flexibleCheck() {
        return flexibleFunc + "(" + receiver + ".ApiVersion)";
    }

    /** The Go type used for a struct field, taking the path-derived nested struct name into account. */
    private String goType(String parentStruct, Field field) {
        if (hasFields(field)) {
            String nested = nestedStructName(parentStruct, field);
            return TypeUtils.isArrayType(field.getType()) ? "*[]" + nested : "*" + nested;
        }
        return TypeUtils.toGoType(field.getType());
    }

    /** {@code Parent + SingularField}, e.g. (MetadataResponseTopic, Partitions) -> MetadataResponseTopicPartition. */
    private String nestedStructName(String parentStruct, Field field) {
        return TypeUtils.nestedStructName(parentStruct, field);
    }

    /**
     * Assigns a unique base name to every struct-typed field, used for its {@code <base>Encoder} /
     * {@code <base>Decoder} / {@code taggedFieldsEncoder<Base>} methods. The short field-name base
     * (e.g. {@code partitions}) is used when it is unique within the message; if two struct fields in
     * different places share a name, the later one falls back to the path-derived nested struct name
     * (which is unique by construction) so the generated methods never collide.
     */
    private void assignMethodBaseNames(String parentStruct, List<Field> fields, Set<String> used) {
        for (Field field : fields) {
            if (!hasFields(field)) {
                continue;
            }
            String nested = nestedStructName(parentStruct, field);
            String base = lowerFirst(field.getName());
            if (used.contains(base)) {
                base = lowerFirst(nested);
                int n = 2;
                String candidate = base;
                while (used.contains(candidate)) {
                    candidate = base + (n++);
                }
                base = candidate;
            }
            used.add(base);
            methodBaseNames.put(field, base);
            assignMethodBaseNames(nested, field.getFields(), used);
        }
    }

    /** The unique encoder/decoder base for a struct-typed field (see {@link #assignMethodBaseNames}). */
    private String methodBase(Field field) {
        String base = methodBaseNames.get(field);
        return base != null ? base : lowerFirst(field.getName());
    }

    /** Encoder/decoder method name, e.g. (Partitions, "Encoder") -> partitionsEncoder. */
    private String methodName(Field field, String suffix) {
        return methodBase(field) + suffix;
    }

    private String lowerFirst(String name) {
        return Character.toLowerCase(name.charAt(0)) + name.substring(1);
    }

    /** A local variable name for a field: lower-cased, with a suffix if it would clash with a keyword. */
    private String safeVar(String name) {
        String v = name.toLowerCase();
        return GO_KEYWORDS.contains(v) ? v + "Val" : v;
    }

    private String versionCondition(String versions) {
        if (versions == null || versions.isEmpty()) {
            return null;
        }
        int start = VersionUtils.getStartVersion(versions);
        int end = VersionUtils.getEndVersion(versions);

        if (end != NO_END && start == end) {
            return receiver + ".ApiVersion == " + start;
        }

        List<String> parts = new ArrayList<>();
        if (start > 0) {
            parts.add(receiver + ".ApiVersion >= " + start);
        }
        if (end != NO_END) {
            parts.add(receiver + ".ApiVersion <= " + end);
        }
        return parts.isEmpty() ? null : String.join(" && ", parts);
    }

    private boolean hasFields(Field field) {
        return field.getFields() != null && !field.getFields().isEmpty();
    }

    /** Pointer-typed fields are strings, bytes, records, arrays and nested structs. */
    private boolean isPointerType(Field field) {
        String type = field.getType();
        return type.equals("string")
                || type.equals("bytes")
                || type.equals("records")
                || TypeUtils.isArrayType(type)
                || hasFields(field);
    }

    private boolean hasTaggedFields(List<Field> fields) {
        return fields.stream().anyMatch(f -> f.getTag() != null);
    }

    private List<Field> taggedFields(List<Field> fields) {
        return fields.stream().filter(f -> f.getTag() != null).collect(Collectors.toList());
    }

    private String capitalize(String name) {
        return TypeUtils.toCamelCase(name);
    }

    private String spaces(int level) {
        return "    ".repeat(level);
    }
}
