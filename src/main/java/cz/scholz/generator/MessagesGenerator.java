package cz.scholz.generator;

import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.util.TypeUtils;
import cz.scholz.generator.util.VersionUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Generates the {@code messages} leaf package: a registry that maps Kafka API keys to the generated
 * request/response body structs, together with human-readable API names, named API-key constants,
 * and the supported version range for each API.
 *
 * <p>This deliberately lives in its own package because it imports every {@code api/*} package. It
 * cannot be folded into {@code protocol} or {@code apis} without creating an import cycle
 * ({@code api/* -> protocol -> apis}). The body interfaces it returns ({@code protocol.RequestBody}
 * / {@code protocol.ResponseBody}) are hand-written in the {@code protocol} package.
 */
public class MessagesGenerator {
    private static final String MODULE = "github.com/scholzj/go-kafka-protocol";

    private final List<ApiSpec> specs;

    public MessagesGenerator(List<ApiSpec> specs) {
        this.specs = specs;
    }

    public String generate() {
        // Group specs by API key, keeping request and response sides separate.
        Map<Integer, ApiSpec> requestSpecs = new TreeMap<>();
        Map<Integer, ApiSpec> responseSpecs = new TreeMap<>();

        for (ApiSpec spec : specs) {
            if (spec.getApiKey() == null) {
                continue;
            }
            if ("request".equals(spec.getType())) {
                requestSpecs.put(spec.getApiKey(), spec);
            } else if ("response".equals(spec.getType())) {
                responseSpecs.put(spec.getApiKey(), spec);
            }
        }

        // Union of all API keys - normally every API has both a request and a response.
        Set<Integer> apiKeys = new TreeSet<>();
        apiKeys.addAll(requestSpecs.keySet());
        apiKeys.addAll(responseSpecs.keySet());

        // The api/* packages we need to import. One package holds both the request and the response
        // for an API, so a single import covers both switch statements.
        Set<String> packages = new TreeSet<>();
        for (ApiSpec spec : requestSpecs.values()) {
            packages.add(TypeUtils.toPackageName(spec.getName()));
        }
        for (ApiSpec spec : responseSpecs.values()) {
            packages.add(TypeUtils.toPackageName(spec.getName()));
        }

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);

        pw.println("package messages");
        pw.println();
        pw.println("import (");
        pw.println("    \"" + MODULE + "/protocol\"");
        pw.println();
        for (String pkg : packages) {
            pw.println("    \"" + MODULE + "/api/" + pkg + "\"");
        }
        pw.println(")");
        pw.println();

        // Named API-key constants, e.g. Metadata int16 = 3.
        pw.println("// Named constants for every supported Kafka API key.");
        pw.println("const (");
        for (Integer apiKey : apiKeys) {
            pw.println("    " + apiName(apiKey, requestSpecs, responseSpecs) + " int16 = " + apiKey);
        }
        pw.println(")");
        pw.println();

        // NewRequestBody.
        pw.println("// NewRequestBody returns an empty request body struct for the given API key, ready to be");
        pw.println("// populated via its Read method. The boolean is false for unknown API keys.");
        pw.println("func NewRequestBody(apiKey int16) (protocol.RequestBody, bool) {");
        pw.println("    switch apiKey {");
        for (Map.Entry<Integer, ApiSpec> entry : requestSpecs.entrySet()) {
            ApiSpec spec = entry.getValue();
            String pkg = TypeUtils.toPackageName(spec.getName());
            pw.println("    case " + entry.getKey() + ":");
            pw.println("        return &" + pkg + "." + spec.getName() + "{}, true");
        }
        pw.println("    default:");
        pw.println("        return nil, false");
        pw.println("    }");
        pw.println("}");
        pw.println();

        // NewResponseBody.
        pw.println("// NewResponseBody returns an empty response body struct for the given API key, ready to be");
        pw.println("// populated via its Read method. The boolean is false for unknown API keys.");
        pw.println("func NewResponseBody(apiKey int16) (protocol.ResponseBody, bool) {");
        pw.println("    switch apiKey {");
        for (Map.Entry<Integer, ApiSpec> entry : responseSpecs.entrySet()) {
            ApiSpec spec = entry.getValue();
            String pkg = TypeUtils.toPackageName(spec.getName());
            pw.println("    case " + entry.getKey() + ":");
            pw.println("        return &" + pkg + "." + spec.getName() + "{}, true");
        }
        pw.println("    default:");
        pw.println("        return nil, false");
        pw.println("    }");
        pw.println("}");
        pw.println();

        // Name.
        pw.println("// Name returns the human-readable name for the given API key (for example \"Metadata\"),");
        pw.println("// or \"Unknown\" if the API key is not recognised.");
        pw.println("func Name(apiKey int16) string {");
        pw.println("    switch apiKey {");
        for (Integer apiKey : apiKeys) {
            pw.println("    case " + apiKey + ":");
            pw.println("        return \"" + apiName(apiKey, requestSpecs, responseSpecs) + "\"");
        }
        pw.println("    default:");
        pw.println("        return \"Unknown\"");
        pw.println("    }");
        pw.println("}");
        pw.println();

        // VersionRange.
        pw.println("// VersionRange returns the minimum and maximum API versions supported by the generated");
        pw.println("// code for the given API key. The boolean is false for unknown API keys. A proxy can use");
        pw.println("// this to fall open - forwarding a body raw rather than attempting a decode it cannot");
        pw.println("// round-trip - when a client and broker negotiate a version newer than this code knows.");
        pw.println("func VersionRange(apiKey int16) (minVersion int16, maxVersion int16, ok bool) {");
        pw.println("    switch apiKey {");
        for (Integer apiKey : apiKeys) {
            ApiSpec spec = requestSpecs.containsKey(apiKey) ? requestSpecs.get(apiKey) : responseSpecs.get(apiKey);
            int min = VersionUtils.getStartVersion(spec.getValidVersions());
            int max = VersionUtils.getEndVersion(spec.getValidVersions());
            pw.println("    case " + apiKey + ":");
            pw.println("        return " + min + ", " + max + ", true");
        }
        pw.println("    default:");
        pw.println("        return 0, 0, false");
        pw.println("    }");
        pw.println("}");

        return sw.toString();
    }

    /** Converts a struct name to the bare API name: MetadataResponse -> Metadata. */
    private String apiName(int apiKey, Map<Integer, ApiSpec> requestSpecs, Map<Integer, ApiSpec> responseSpecs) {
        ApiSpec spec = requestSpecs.containsKey(apiKey) ? requestSpecs.get(apiKey) : responseSpecs.get(apiKey);
        return spec.getName().replace("Request", "").replace("Response", "");
    }
}
