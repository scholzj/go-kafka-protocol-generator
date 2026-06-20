package cz.scholz.generator;

import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.model.Field;
import cz.scholz.generator.util.TypeUtils;
import cz.scholz.generator.util.VersionUtils;

import java.util.List;

/**
 * Generates a Go round-trip test for a single Kafka protocol message, written next to the message
 * itself as {@code request_test.go} / {@code response_test.go}.
 *
 * <p>The test builds one fully-populated instance of the message and, for every protocol version in
 * the message's valid range, runs a <b>write &rarr; read &rarr; write</b> cycle and asserts the two
 * encodings are byte-identical. Comparing the re-encoded bytes (rather than the structs) makes the
 * test robust against nil-vs-empty differences in fields such as {@code rawTaggedFields}, while still
 * exercising the full {@code Write}/{@code Read} path - every field, encoder, decoder, tagged-fields
 * handler and version guard - at each version. A field that is not present at a given version is
 * simply never written, so a single fully-populated instance is valid for the whole range.
 *
 * <p>Note this only catches <em>asymmetric</em> encode/decode bugs; wire-format correctness against a
 * real broker is the job of the hand-written golden tests.
 */
public class GoTestGenerator {
    private static final int NO_END = Integer.MAX_VALUE;

    private final ApiSpec spec;
    private final String packageName;
    private final String mainStruct;
    private final boolean isRequest;
    private final String paramType;     // "Request" or "Response"
    private final String ptrHelper;     // reqPtr / resPtr
    private final int minVersion;
    private final int maxVersion;

    private final StringBuilder sb = new StringBuilder();

    public GoTestGenerator(ApiSpec spec) {
        this.spec = spec;
        this.packageName = TypeUtils.toPackageName(spec.getName());
        this.mainStruct = spec.getName();
        this.isRequest = "request".equals(spec.getType());
        this.paramType = isRequest ? "Request" : "Response";
        this.ptrHelper = (isRequest ? "req" : "res") + "Ptr";
        this.minVersion = VersionUtils.getStartVersion(spec.getValidVersions());
        this.maxVersion = computeMaxVersion();
    }

    public String generate() {
        line(0, "package " + packageName);
        blank();
        line(0, "import (");
        line(1, "\"bytes\"");
        line(1, "\"testing\"");
        line(1, "\"github.com/scholzj/go-kafka-protocol/protocol\"");
        if (TypeUtils.needsUUID(spec)) {
            line(1, "\"github.com/google/uuid\"");
        }
        line(0, ")");
        blank();

        // A small generic helper to take the address of a literal (e.g. for *string fields).
        line(0, "func " + ptrHelper + "[T any](v T) *T { return &v }");
        blank();

        String param = paramType.toLowerCase();
        line(0, "// Round-trips a fully populated " + mainStruct + " through Write/Read/Write at every");
        line(0, "// valid protocol version and checks the re-encoded bytes match.");
        line(0, "func Test" + mainStruct + "RoundTrip(t *testing.T) {");
        line(1, "in := &" + structLiteral(mainStruct, spec.getFields()));
        blank();
        line(1, "for v := int16(" + minVersion + "); v <= " + maxVersion + "; v++ {");
        line(2, "in.ApiVersion = v");
        blank();
        line(2, "var buf bytes.Buffer");
        line(2, "if err := in.Write(&buf); err != nil {");
        line(3, "t.Fatalf(\"v%d: write: %v\", v, err)");
        line(2, "}");
        line(2, "encoded := buf.Bytes()");
        blank();
        line(2, "out := &" + mainStruct + "{}");
        line(2, param + " := &protocol." + paramType + "{Body: bytes.NewBuffer(encoded)}");
        line(2, param + ".ApiVersion = v");
        line(2, "if err := out.Read(" + param + "); err != nil {");
        line(3, "t.Fatalf(\"v%d: read: %v\", v, err)");
        line(2, "}");
        blank();
        line(2, "var reencoded bytes.Buffer");
        line(2, "if err := out.Write(&reencoded); err != nil {");
        line(3, "t.Fatalf(\"v%d: re-write: %v\", v, err)");
        line(2, "}");
        line(2, "if !bytes.Equal(encoded, reencoded.Bytes()) {");
        line(3, "t.Errorf(\"v%d: round-trip mismatch:\\n  encoded:   %x\\n  reencoded: %x\", v, encoded, reencoded.Bytes())");
        line(2, "}");
        blank();
        line(2, "// PrettyPrint must not panic, for the populated and the zero value alike.");
        line(2, "_ = in.PrettyPrint()");
        line(2, "_ = (&" + mainStruct + "{}).PrettyPrint()");
        line(1, "}");
        line(0, "}");

        return sb.toString();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Value builders
    ////////////////////////////////////////////////////////////////////////////

    /** A composite literal for a struct: {@code StructName{Field: value, ...}} (gofmt re-indents). */
    private String structLiteral(String structName, List<Field> fields) {
        StringBuilder lit = new StringBuilder(structName).append("{\n");
        for (Field field : fields) {
            lit.append("\t").append(TypeUtils.toCamelCase(field.getName()))
               .append(": ").append(fieldValue(structName, field)).append(",\n");
        }
        lit.append("}");
        return lit.toString();
    }

    /** A valid value expression for a single field. */
    private String fieldValue(String parentStruct, Field field) {
        String type = field.getType();

        if (TypeUtils.isArrayType(type)) {
            String elem = TypeUtils.getArrayElementType(type);
            if (hasFields(field)) {
                String nested = TypeUtils.nestedStructName(parentStruct, field);
                return "&[]" + nested + "{" + structLiteral(nested, field.getFields()) + "}";
            }
            return "&[]" + elementGoType(elem) + "{" + scalarLiteral(elem) + "}";
        }
        if (hasFields(field)) {
            String nested = TypeUtils.nestedStructName(parentStruct, field);
            return "&" + structLiteral(nested, field.getFields());
        }
        switch (type) {
            case "string":
                return ptrHelper + "(\"x\")";
            case "bytes":
            case "records":
                return "&[]byte{1, 2, 3}";
            default:
                return scalarLiteral(type);
        }
    }

    /** A literal for a value-typed scalar (also used as an array element). */
    private String scalarLiteral(String type) {
        switch (type) {
            case "bool":
                return "true";
            case "uuid":
                return "uuid.UUID{}";
            case "string":
                return "\"x\"";
            default:
                // int8 / int16 / int32 / int64 - an untyped constant fits all of them.
                return "1";
        }
    }

    /** The Go element type used inside a primitive slice literal. */
    private String elementGoType(String kafkaType) {
        switch (kafkaType) {
            case "bool":
                return "bool";
            case "uuid":
                return "uuid.UUID";
            case "string":
                return "string";
            default:
                return kafkaType; // int8 / int16 / int32 / int64
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Versions
    ////////////////////////////////////////////////////////////////////////////

    /** The highest version worth exercising: the highest version referenced anywhere in the spec,
     *  capped at the end of {@code validVersions}. */
    private int computeMaxVersion() {
        int max = minVersion;
        int flexStart = VersionUtils.getStartVersion(spec.getFlexibleVersions());
        if (flexStart != NO_END) {
            max = Math.max(max, flexStart);
        }
        max = Math.max(max, maxFieldVersion(spec.getFields()));

        int validEnd = VersionUtils.getEndVersion(spec.getValidVersions());
        if (validEnd != NO_END) {
            max = Math.min(max, validEnd);
        }
        return max;
    }

    private int maxFieldVersion(List<Field> fields) {
        int max = 0;
        for (Field field : fields) {
            if (field.getVersions() != null) {
                max = Math.max(max, VersionUtils.getStartVersion(field.getVersions()));
                int end = VersionUtils.getEndVersion(field.getVersions());
                if (end != NO_END) {
                    max = Math.max(max, end);
                }
            }
            if (field.getNullableVersions() != null) {
                max = Math.max(max, VersionUtils.getStartVersion(field.getNullableVersions()));
            }
            if (hasFields(field)) {
                max = Math.max(max, maxFieldVersion(field.getFields()));
            }
        }
        return max;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Helpers
    ////////////////////////////////////////////////////////////////////////////

    private boolean hasFields(Field field) {
        return field.getFields() != null && !field.getFields().isEmpty();
    }

    private void line(int indent, String text) {
        sb.append("\t".repeat(indent)).append(text).append("\n");
    }

    private void blank() {
        sb.append("\n");
    }
}
