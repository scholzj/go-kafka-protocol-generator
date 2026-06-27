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
            writeSingleStruct(field, ref, indent);
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
     * Emits the write for a single nested struct. A nullable struct is preceded by an int8 marker
     * ({@code -1} for null, {@code 1} for present) exactly as Kafka encodes it; a non-nullable struct
     * (or a non-nullable version of a partially-nullable one) is written inline - {@link #emitNullCheck}
     * has already rejected a nil pointer in those versions.
     */
    private void writeSingleStruct(Field field, String ref, int indent) {
        String encoder = receiver + "." + methodName(field, "Encoder");
        String nullableCond = nullableVersionCondition(field);
        if (nullableCond == null) {
            writeStructEncode(encoder, ref, indent);
        } else if (nullableCond.isEmpty()) {
            writeNullableStructBody(encoder, ref, indent);
        } else {
            line(indent, "if " + nullableCond + " {");
            writeNullableStructBody(encoder, ref, indent + 1);
            line(indent, "} else {");
            writeStructEncode(encoder, ref, indent + 1);
            line(indent, "}");
        }
    }

    private void writeNullableStructBody(String encoder, String ref, int indent) {
        line(indent, "if " + ref + " == nil {");
        writeSimple("protocol.WriteInt8(w, -1)", indent + 1);
        line(indent, "} else {");
        writeSimple("protocol.WriteInt8(w, 1)", indent + 1);
        writeStructEncode(encoder, ref, indent + 1);
        line(indent, "}");
    }

    private void writeStructEncode(String encoder, String ref, int indent) {
        line(indent, "if err := " + encoder + "(w, *" + ref + "); err != nil {");
        line(indent + 1, "return err");
        line(indent, "}");
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

    /**
     * Returns the version predicate (using the receiver's ApiVersion) selecting the versions in which
     * a field is nullable - i.e. where a nullable single struct needs its int8 null marker, or where
     * an array must use the nullable (null-accepting) reader rather than the strict one:
     * <ul>
     *   <li>{@code null} - the field is never nullable in any version it is present;</li>
     *   <li>{@code ""}   - nullable in every version it is present;</li>
     *   <li>a Go boolean expression - nullable only in the versions it matches.</li>
     * </ul>
     * This is the complement of {@link #nonNullableVersionPredicate}.
     */
    private String nullableVersionCondition(Field field) {
        if (field.getNullableVersions() == null) {
            return null;
        }

        int pStart = VersionUtils.getStartVersion(field.getVersions());
        int pEnd = VersionUtils.getEndVersion(field.getVersions());
        int nStart = VersionUtils.getStartVersion(field.getNullableVersions());
        int nEnd = VersionUtils.getEndVersion(field.getNullableVersions());

        if (nStart <= pStart && pEnd <= nEnd) {
            return ""; // nullable everywhere the field exists
        }
        if (pEnd < nStart || pStart > nEnd) {
            return null; // the nullable range never overlaps the field's versions
        }

        // Partial overlap: nullable only inside the intersection of the two ranges.
        List<String> parts = new ArrayList<>();
        if (nStart > pStart) {
            parts.add(receiver + ".ApiVersion >= " + nStart);
        }
        if (nEnd < pEnd) {
            parts.add(receiver + ".ApiVersion <= " + nEnd);
        }
        return parts.size() == 1 ? parts.get(0) : "(" + String.join(" && ", parts) + ")";
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
        // Reset the receiver so a reused struct does not keep stale values: positional fields are
        // always overwritten, but a tagged field whose tag is absent (or a field absent at this
        // version) would otherwise retain whatever a previous Read left behind, including
        // rawTaggedFields. Starting from the zero value also gives every tagged field its Kafka
        // default (the explicit non-zero ones are then re-applied below).
        line(1, "*" + receiver + " = " + mainStruct + "{}");
        blank();
        line(1, "r := bytes.NewBuffer(" + param + ".Body.Bytes())");
        line(1, receiver + ".ApiVersion = " + param + ".ApiVersion");
        blank();

        emitDefaultInits(spec.getFields(), receiver, mainStruct, 1);

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
            readArray(var, dst, decoderCompact, decoderPlain, nullableVersionCondition(field), errReturn, indent);
        } else if (hasFields(field)) {
            // Single nested struct.
            readSingleStruct(field, var, dst, errReturn, indent);
        } else if (type.equals("string")) {
            readLengthPrefixed(field, var, dst, errReturn, indent, "String");
        } else if (type.equals("bytes")) {
            readLengthPrefixed(field, var, dst, errReturn, indent, "Bytes");
        } else if (type.equals("records")) {
            // Records use the compact length only in flexible versions (decided at runtime); the
            // nullable vs strict (null-rejecting) reader is chosen per version like strings/arrays.
            readRecords(field, var, dst, errReturn, indent);
        } else {
            String method = TypeUtils.getProtocolReadMethod(type, false, false, false);
            line(indent, var + ", err := protocol." + method + "(r)");
            readErrCheck(errReturn, indent);
            line(indent, dst + " = " + var);
        }
    }

    /**
     * Reads a single nested struct, mirroring {@link #writeSingleStruct}. A nullable struct is
     * preceded by an int8 marker ({@code < 0} means null); a non-nullable struct is decoded inline.
     */
    private void readSingleStruct(Field field, String var, String dst, String errReturn, int indent) {
        String decoder = receiver + "." + methodName(field, "Decoder");
        String nullableCond = nullableVersionCondition(field);
        if (nullableCond == null) {
            readStructDecode(decoder, var, dst, errReturn, indent);
        } else if (nullableCond.isEmpty()) {
            readNullableStructBody(decoder, var, dst, errReturn, indent);
        } else {
            line(indent, "if " + nullableCond + " {");
            readNullableStructBody(decoder, var, dst, errReturn, indent + 1);
            line(indent, "} else {");
            readStructDecode(decoder, var, dst, errReturn, indent + 1);
            line(indent, "}");
        }
    }

    private void readNullableStructBody(String decoder, String var, String dst, String errReturn, int indent) {
        line(indent, var + "Flag, err := protocol.ReadInt8(r)");
        readErrCheck(errReturn, indent);
        line(indent, "if " + var + "Flag >= 0 {");
        readStructDecode(decoder, var, dst, errReturn, indent + 1);
        line(indent, "} else {");
        line(indent + 1, dst + " = nil");
        line(indent, "}");
    }

    private void readStructDecode(String decoder, String var, String dst, String errReturn, int indent) {
        line(indent, var + ", err := " + decoder + "(r)");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = &" + var);
    }

    /**
     * Reads an array, choosing the nullable or the strict (null-rejecting) reader per version.
     * {@code nullableCond} is the {@link #nullableVersionCondition} for the field: {@code null} means
     * never nullable (always strict), {@code ""} means nullable across its whole range (always
     * nullable), and a Go expression means partial - nullable only in the matching versions, strict
     * elsewhere, so a wire null is rejected exactly in the versions where the field is non-nullable.
     */
    private void readArray(String var, String dst, String decoderCompact, String decoderPlain, String nullableCond, String errReturn, int indent) {
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            readArrayChoice(var, dst, decoderCompact, nullableCond, errReturn, indent + 1, true);
            line(indent, "} else {");
            readArrayChoice(var, dst, decoderPlain, nullableCond, errReturn, indent + 1, false);
            line(indent, "}");
        } else {
            readArrayChoice(var, dst, decoderPlain, nullableCond, errReturn, indent, false);
        }
    }

    /** Emits the nullable/strict array read for one encoding (compact or plain), version-split when the
     *  field is only partially nullable. */
    private void readArrayChoice(String var, String dst, String decoder, String nullableCond, String errReturn, int indent, boolean compact) {
        if (nullableCond == null) {
            readArrayStrict(var, dst, decoder, errReturn, indent, compact);
        } else if (nullableCond.isEmpty()) {
            readArrayNullable(var, dst, decoder, errReturn, indent, compact);
        } else {
            line(indent, "if " + nullableCond + " {");
            readArrayNullable(var, dst, decoder, errReturn, indent + 1, compact);
            line(indent, "} else {");
            readArrayStrict(var, dst, decoder, errReturn, indent + 1, compact);
            line(indent, "}");
        }
    }

    /** Nullable array read: the helper returns {@code *[]T}, assignable directly. */
    private void readArrayNullable(String var, String dst, String decoder, String errReturn, int indent, boolean compact) {
        String reader = compact ? "ReadNullableCompactArray" : "ReadNullableArray";
        line(indent, var + ", err := protocol." + reader + "(r, " + decoder + ")");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = " + var);
    }

    /** Strict (null-rejecting) array read: the helper returns {@code []T}, which we address. */
    private void readArrayStrict(String var, String dst, String decoder, String errReturn, int indent, boolean compact) {
        String reader = compact ? "ReadCompactArray" : "ReadArray";
        line(indent, var + ", err := protocol." + reader + "(r, " + decoder + ")");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = &" + var);
    }

    /**
     * Reads a length-prefixed pointer type ({@code word} is "String" or "Bytes"), choosing the
     * nullable or the strict (null-rejecting) reader per version - mirroring {@link #readArray}.
     * {@code nullableCond} is the {@link #nullableVersionCondition}: {@code null} = always strict,
     * {@code ""} = always nullable, a Go expression = nullable only in the matching versions (so a
     * wire null is rejected exactly in the versions where the field is non-nullable).
     */
    private void readLengthPrefixed(Field field, String var, String dst, String errReturn, int indent, String word) {
        String nullableCond = nullableVersionCondition(field);
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            readLengthPrefixedChoice(var, dst, word, nullableCond, errReturn, indent + 1, true);
            line(indent, "} else {");
            readLengthPrefixedChoice(var, dst, word, nullableCond, errReturn, indent + 1, false);
            line(indent, "}");
        } else {
            readLengthPrefixedChoice(var, dst, word, nullableCond, errReturn, indent, false);
        }
    }

    private void readLengthPrefixedChoice(String var, String dst, String word, String nullableCond, String errReturn, int indent, boolean compact) {
        if (nullableCond == null) {
            readLengthPrefixedStrict(var, dst, word, errReturn, indent, compact);
        } else if (nullableCond.isEmpty()) {
            readLengthPrefixedNullable(var, dst, word, errReturn, indent, compact);
        } else {
            line(indent, "if " + nullableCond + " {");
            readLengthPrefixedNullable(var, dst, word, errReturn, indent + 1, compact);
            line(indent, "} else {");
            readLengthPrefixedStrict(var, dst, word, errReturn, indent + 1, compact);
            line(indent, "}");
        }
    }

    /** Nullable string/bytes read: the helper returns a pointer, assignable directly. */
    private void readLengthPrefixedNullable(String var, String dst, String word, String errReturn, int indent, boolean compact) {
        String reader = (compact ? "ReadNullableCompact" : "ReadNullable") + word;
        line(indent, var + ", err := protocol." + reader + "(r)");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = " + var);
    }

    /** Strict (null-rejecting) string/bytes read: the helper returns a value, which we address. */
    private void readLengthPrefixedStrict(String var, String dst, String word, String errReturn, int indent, boolean compact) {
        String reader = (compact ? "ReadCompact" : "Read") + word;
        line(indent, var + ", err := protocol." + reader + "(r)");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = &" + var);
    }

    /**
     * Reads a records field, choosing the compact encoding in flexible versions and the nullable vs
     * strict (null-rejecting) reader per version - mirroring {@link #readLengthPrefixed}. Records are
     * length-prefixed like bytes; the nullable helpers return {@code *[]byte}, the strict ones
     * {@code []byte}.
     */
    private void readRecords(Field field, String var, String dst, String errReturn, int indent) {
        String nullableCond = nullableVersionCondition(field);
        if (flexible) {
            line(indent, "if " + flexibleCheck() + " {");
            readRecordsChoice(var, dst, nullableCond, errReturn, indent + 1, true);
            line(indent, "} else {");
            readRecordsChoice(var, dst, nullableCond, errReturn, indent + 1, false);
            line(indent, "}");
        } else {
            readRecordsChoice(var, dst, nullableCond, errReturn, indent, false);
        }
    }

    private void readRecordsChoice(String var, String dst, String nullableCond, String errReturn, int indent, boolean compact) {
        if (nullableCond == null) {
            readRecordsStrict(var, dst, errReturn, indent, compact);
        } else if (nullableCond.isEmpty()) {
            readRecordsNullable(var, dst, errReturn, indent, compact);
        } else {
            line(indent, "if " + nullableCond + " {");
            readRecordsNullable(var, dst, errReturn, indent + 1, compact);
            line(indent, "} else {");
            readRecordsStrict(var, dst, errReturn, indent + 1, compact);
            line(indent, "}");
        }
    }

    /** Nullable records read: the helper returns {@code *[]byte}, assignable directly. */
    private void readRecordsNullable(String var, String dst, String errReturn, int indent, boolean compact) {
        String reader = compact ? "ReadCompactRecords" : "ReadRecords";
        line(indent, var + ", err := protocol." + reader + "(r)");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = " + var);
    }

    /** Strict (null-rejecting) records read: the helper returns {@code []byte}, which we address. */
    private void readRecordsStrict(String var, String dst, String errReturn, int indent, boolean compact) {
        String reader = compact ? "ReadCompactRecordsStrict" : "ReadRecordsStrict";
        line(indent, var + ", err := protocol." + reader + "(r)");
        readErrCheck(errReturn, indent);
        line(indent, dst + " = &" + var);
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
        emitDefaultInits(field.getFields(), var, nested, 1);
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

        // A tagged field is serialised only when the ApiVersion is within its taggedVersions range
        // AND its value differs from the default (Kafka omits a tag whose value equals the default,
        // and a pointer/nullable tag whose value is nil). Both conditions are combined into one guard.
        List<String> conds = new ArrayList<>();
        String verCond = versionCondition(taggedFieldVersions(field));
        if (verCond != null) {
            conds.add(verCond);
        }
        String valCond = taggedEmitValueCondition(field, ref);
        if (valCond != null) {
            conds.add(valCond);
        }

        int indent = 1;
        if (!conds.isEmpty()) {
            line(1, "if " + String.join(" && ", conds) + " {");
            indent = 2;
        }
        line(indent, "buf = bytes.NewBuffer(make([]byte, 0))");
        emitTaggedValueWrite(field, ref, indent);
        blank();
        line(indent, "taggedFields = append(taggedFields, protocol.TaggedField{Tag: " + field.getTag() + ", Field: buf.Bytes()})");
        if (!conds.isEmpty()) {
            line(1, "}");
        }
        blank();
    }

    /**
     * Emits, at the top of a Read/Decoder, the initialisation of every field whose Kafka default is
     * not already the Go zero value the receiver reset provides. A field absent from the wire (a tag
     * that is not present, or a positional field outside its version range) keeps its default:
     * <ul>
     *   <li>value fields with a non-zero default (e.g. {@code -1} or a bool {@code true}) are set to it;</li>
     *   <li>non-nullable <b>tagged</b> array fields default to an empty list (Kafka's collection
     *       default) rather than nil;</li>
     *   <li>non-nullable <b>tagged</b> scalar-only struct fields default to a struct with its
     *       sub-field defaults, matching the instance Kafka materialises for an omitted tag.</li>
     * </ul>
     * Nullable fields default to nil (already provided by the reset).
     */
    private void emitDefaultInits(List<Field> fields, String target, String parentStruct, int indent) {
        List<String> inits = new ArrayList<>();
        for (Field f : fields) {
            String lit = defaultInitLiteral(parentStruct, f);
            if (lit != null) {
                inits.add(target + "." + capitalize(f.getName()) + " = " + lit);
            }
        }
        if (inits.isEmpty()) {
            return;
        }
        line(indent, "// Field defaults (applied before decode; a field absent from the wire keeps its default)");
        for (String s : inits) {
            line(indent, s);
        }
        blank();
    }

    /**
     * The Go literal a field must be initialised to before decode so that, if it is absent from the
     * wire, it carries its Kafka default - or {@code null} when the default is the Go zero value the
     * receiver reset already gives it (a zero-default value, or a nil pointer/nullable field).
     */
    private String defaultInitLiteral(String parentStruct, Field field) {
        if (!isPointerType(field)) {
            return nonZeroDefaultLiteral(field);
        }
        // Only non-nullable tagged pointer fields have a non-nil Kafka default worth materialising; a
        // nullable field defaults to null (nil), and a non-tagged field is read whenever it is present.
        if (field.getTag() == null || field.getNullableVersions() != null) {
            return null;
        }
        if (TypeUtils.isArrayType(field.getType())) {
            return "&" + goType(parentStruct, field).substring(1) + "{}"; // *[]T -> &[]T{}
        }
        if (hasFields(field) && allScalarSubfields(field)) {
            String nested = goType(parentStruct, field).substring(1); // *Nested -> Nested
            List<String> subInits = new ArrayList<>();
            for (Field sub : field.getFields()) {
                String d = nonZeroDefaultLiteral(sub);
                if (d != null) {
                    subInits.add(capitalize(sub.getName()) + ": " + d);
                }
            }
            return "&" + nested + "{" + String.join(", ", subInits) + "}";
        }
        return null; // tagged string/bytes/records (and non-scalar structs): leave nil
    }

    /** The Go literal for a value-typed field's non-zero Kafka default, or {@code null} when the
     *  default is the type's zero value (so no explicit initialisation is needed). */
    private String nonZeroDefaultLiteral(Field field) {
        String type = field.getType();
        String def = field.getDefaultValue();
        switch (type) {
            case "bool":
                return "true".equalsIgnoreCase(def) ? "true" : null;
            case "uuid":
                return null; // zero uuid default
            default: // int8 / int16 / int32 / int64
                if (def != null && !def.isEmpty() && !def.equals("0")) {
                    return def;
                }
                return null;
        }
    }

    /** The versions in which a tagged field is serialised as a tag: its {@code taggedVersions} if the
     *  spec gives one, otherwise its {@code versions}. */
    private String taggedFieldVersions(Field field) {
        return field.getTaggedVersions() != null ? field.getTaggedVersions() : field.getVersions();
    }

    /**
     * The Go condition under which a tagged field's value must be serialised (it differs from the
     * Kafka default Kafka would otherwise omit), or {@code null} when there is no value-based guard:
     * <ul>
     *   <li>a <b>non-nullable array</b> - the default is an empty array, so emit only when non-nil
     *       <em>and</em> non-empty;</li>
     *   <li>a <b>nullable array</b> - the default is null, so nil and empty are distinct: emit
     *       whenever non-nil (an empty-but-present array still differs from null);</li>
     *   <li>a <b>single struct</b> whose sub-fields are all scalars - the default is the struct with
     *       every sub-field at its default, so emit when non-nil and at least one sub-field differs;
     *       a struct with non-scalar sub-fields falls back to non-nil (its default is not modelled);</li>
     *   <li>other <b>pointer/nullable</b> tags (string, bytes, records) - emit when non-nil;</li>
     *   <li>a <b>value</b> tag - emit when it differs from its default (see {@link #valueDiffersFromDefault}).</li>
     * </ul>
     */
    private String taggedEmitValueCondition(Field field, String ref) {
        if (TypeUtils.isArrayType(field.getType())) {
            if (field.getNullableVersions() != null) {
                return ref + " != nil"; // nullable: null is the default, empty differs from it
            }
            return ref + " != nil && len(*" + ref + ") > 0"; // non-nullable: empty is the default
        }
        if (hasFields(field)) {
            // Single nested struct: model its default (all sub-fields at their defaults) when every
            // sub-field is a scalar value type; otherwise just guard on non-nil. Even a struct whose
            // scalar fields are all at their defaults must still be emitted when it carries preserved
            // raw (unknown nested) tagged fields, otherwise those would be dropped on re-encode.
            String structDiff = structDiffersFromDefault(field, ref);
            if (structDiff != null) {
                return ref + " != nil && (" + structDiff
                        + " || (" + ref + ".rawTaggedFields != nil && len(*" + ref + ".rawTaggedFields) > 0))";
            }
            return ref + " != nil";
        }
        if (isPointerType(field)) {
            return ref + " != nil";
        }
        return valueDiffersFromDefault(field, ref);
    }

    /** The Go condition that a value-typed field differs from its Kafka default (an explicit
     *  {@code default} or, failing that, the type's zero value: {@code false} for bool, the zero uuid,
     *  {@code 0} for an int). */
    private String valueDiffersFromDefault(Field field, String ref) {
        String type = field.getType();
        String def = field.getDefaultValue();
        switch (type) {
            case "bool":
                return "true".equalsIgnoreCase(def) ? "!" + ref : ref;
            case "uuid":
                // The literal is parenthesised so the composite-literal braces are not misparsed.
                return ref + " != (uuid.UUID{})";
            default: // int8 / int16 / int32 / int64
                String d = (def != null && !def.isEmpty()) ? def : "0";
                return ref + " != " + d;
        }
    }

    /**
     * The Go condition that a single nested struct differs from its all-defaults value, i.e. an OR of
     * each sub-field differing from its default - or {@code null} when the struct has a non-scalar
     * sub-field (whose default is not modelled) or no sub-fields. Kafka omits a tagged struct that
     * equals a freshly-defaulted instance; this reproduces that test for the common scalar-only case.
     */
    private String structDiffersFromDefault(Field field, String ref) {
        if (!allScalarSubfields(field)) {
            return null; // a non-scalar (or tagged) sub-field: do not model the struct default
        }
        List<String> parts = new ArrayList<>();
        for (Field sub : field.getFields()) {
            parts.add(valueDiffersFromDefault(sub, ref + "." + capitalize(sub.getName())));
        }
        return String.join(" || ", parts);
    }

    /** Whether a struct field has at least one sub-field and every sub-field is a plain scalar value
     *  type (no tag, not a pointer/nullable, not itself a struct) - the case whose default we model. */
    private boolean allScalarSubfields(Field field) {
        if (field.getFields() == null || field.getFields().isEmpty()) {
            return false;
        }
        for (Field sub : field.getFields()) {
            if (sub.getTag() != null || isPointerType(sub) || hasFields(sub)) {
                return false;
            }
        }
        return true;
    }

    /** Writes one tagged field into {@code buf}. Tagged fields only exist in flexible versions, so
     *  the compact encodings are always used. */
    private void emitTaggedValueWrite(Field field, String ref, int indent) {
        String type = field.getType();

        // A single nested struct that is nullable is preceded by Kafka's int8 null marker even when
        // serialised as a tag. A tag is only emitted when its value is present (the surrounding guard
        // is `ref != nil`), so the marker is always 1 here, followed by the struct.
        if (!TypeUtils.isArrayType(type) && hasFields(field)) {
            String encoder = receiver + "." + methodName(field, "Encoder");
            String nullableCond = nullableVersionCondition(field);
            if (nullableCond == null) {
                writeTaggedStmt(encoder + "(buf, *" + ref + ")", indent); // non-nullable: no marker
            } else if (nullableCond.isEmpty()) {
                writeTaggedStmt("protocol.WriteInt8(buf, 1)", indent);
                writeTaggedStmt(encoder + "(buf, *" + ref + ")", indent);
            } else {
                // A tagged single struct whose nullability varies by version is not produced by any
                // current Kafka spec; the in-tag encoding would need verifying against the broker, so
                // fail loudly (this message is reported and skipped) rather than emit wrong bytes.
                throw new UnsupportedOperationException(
                        "tagged single nested struct '" + field.getName()
                        + "' with version-dependent nullability is not supported");
            }
            return;
        }

        String call;
        if (TypeUtils.isArrayType(type)) {
            String encoder = hasFields(field)
                    ? receiver + "." + methodName(field, "Encoder")
                    : "protocol." + TypeUtils.getProtocolWriteMethod(TypeUtils.getArrayElementType(type), false, true, false);
            call = "protocol.WriteNullableCompactArray(buf, " + encoder + ", " + ref + ")";
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
        writeTaggedStmt(call, indent);
    }

    /** Emits {@code if err := <call>; err != nil { return taggedFields, err }} inside a tagged-fields
     *  encoder (where the error path must return the accumulated taggedFields slice). */
    private void writeTaggedStmt(String call, int indent) {
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
        // This decoder is called once per tag by ReadTaggedFields. A tag is "known" only when its
        // number matches a generated case AND the ApiVersion is within that field's taggedVersions
        // range; anything else (a genuinely unknown tag, or a known tag number seen in a version where
        // the field does not exist) is preserved verbatim in rawTaggedFields so it round-trips instead
        // of being decoded into a field and then silently dropped on re-encode. Unknown tags are
        // accumulated into the struct's existing slice rather than a fresh one that would be reassigned
        // (and lost) on every call.
        line(1, "known := false");
        blank();
        line(1, "switch tag {");
        for (Field field : tagged) {
            emitTaggedFieldRead(field, accessor);
        }
        line(1, "}");
        blank();
        line(1, "if !known {");
        line(2, "// Keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)");
        line(2, "field, err := io.ReadAll(r)");
        line(2, "if err != nil {");
        line(3, "return err");
        line(2, "}");
        line(2, "if " + accessor + ".rawTaggedFields == nil {");
        line(3, "rawTaggedFields := make([]protocol.TaggedField, 0)");
        line(3, accessor + ".rawTaggedFields = &rawTaggedFields");
        line(2, "}");
        line(2, "*" + accessor + ".rawTaggedFields = append(*" + accessor + ".rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})");
        line(1, "}");
        blank();
        line(1, "return nil");
    }

    private void emitTaggedFieldRead(Field field, String accessor) {
        line(1, "case " + field.getTag() + ":");
        line(2, "// " + capitalize(field.getName()));
        // Decode only when the ApiVersion is within the field's taggedVersions range; otherwise leave
        // `known` false so the post-switch handler preserves the bytes as a raw tag.
        String verCond = versionCondition(taggedFieldVersions(field));
        if (verCond != null) {
            line(2, "if " + verCond + " {");
            line(3, "known = true");
            emitTaggedFieldReadValue(field, accessor, 3);
            line(2, "}");
        } else {
            line(2, "known = true");
            emitTaggedFieldReadValue(field, accessor, 2);
        }
    }

    /** Emits the decode statements for one tagged field at base indent {@code bi}. */
    private void emitTaggedFieldReadValue(Field field, String accessor, int bi) {
        String type = field.getType();
        String var = safeVar(field.getName());
        String dst = accessor + "." + capitalize(field.getName());

        if (TypeUtils.isArrayType(type)) {
            String decoder = hasFields(field)
                    ? receiver + "." + methodName(field, "Decoder")
                    : "protocol." + TypeUtils.getProtocolReadMethod(TypeUtils.getArrayElementType(type), false, true, false);
            if (field.getNullableVersions() != null) {
                line(bi, var + ", err := protocol.ReadNullableCompactArray(r, " + decoder + ")");
                readErrCheck("return err", bi);
                line(bi, dst + " = " + var); // *[]T
            } else {
                // Non-nullable tag: reject a wire null (length 0) instead of decoding it to nil.
                line(bi, var + ", err := protocol.ReadCompactArray(r, " + decoder + ")");
                readErrCheck("return err", bi);
                line(bi, dst + " = &" + var); // []T
            }
        } else if (hasFields(field)) {
            String decoder = receiver + "." + methodName(field, "Decoder");
            String nullableCond = nullableVersionCondition(field);
            if (nullableCond == null) {
                line(bi, var + "Val, err := " + decoder + "(r)");
                readErrCheck("return err", bi);
                line(bi, dst + " = &" + var + "Val");
            } else if (nullableCond.isEmpty()) {
                // Nullable struct tag: read the int8 marker first (mirrors emitTaggedValueWrite).
                line(bi, var + "Flag, err := protocol.ReadInt8(r)");
                readErrCheck("return err", bi);
                line(bi, "if " + var + "Flag >= 0 {");
                line(bi + 1, var + "Val, err := " + decoder + "(r)");
                readErrCheck("return err", bi + 1);
                line(bi + 1, dst + " = &" + var + "Val");
                line(bi, "} else {");
                line(bi + 1, dst + " = nil");
                line(bi, "}");
            } else {
                throw new UnsupportedOperationException(
                        "tagged single nested struct '" + field.getName()
                        + "' with version-dependent nullability is not supported");
            }
        } else if (type.equals("string") || type.equals("bytes")) {
            String word = type.equals("string") ? "String" : "Bytes";
            boolean nullable = field.getNullableVersions() != null;
            String method = (nullable ? "ReadNullableCompact" : "ReadCompact") + word;
            line(bi, var + ", err := protocol." + method + "(r)");
            readErrCheck("return err", bi);
            line(bi, nullable ? dst + " = " + var : dst + " = &" + var);
        } else if (type.equals("records")) {
            line(bi, var + ", err := protocol.ReadCompactRecords(r)");
            readErrCheck("return err", bi);
            line(bi, dst + " = " + var);
        } else {
            String method = TypeUtils.getProtocolReadMethod(type, false, true, false);
            line(bi, var + ", err := protocol." + method + "(r)");
            readErrCheck("return err", bi);
            line(bi, dst + " = " + var);
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
