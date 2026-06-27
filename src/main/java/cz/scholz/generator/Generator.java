package cz.scholz.generator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.util.CommonStructResolver;
import cz.scholz.generator.util.JsonCommentStripper;
import cz.scholz.generator.util.TypeUtils;

import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * Entry point. Reads the Apache Kafka protocol message definitions straight out of the
 * {@code kafka-clients} JAR (resources under {@code common/message/*.json}) and generates Go code for
 * every request and response into the go-kafka-protocol submodule.
 */
public class Generator {
    // Default output directory (the go-kafka-protocol submodule's api/ folder). apis.go is written to
    // its sibling apis/ directory. Override with a single command-line argument.
    private static final String OUTPUT_DIR = "go-kafka-protocol/api";

    // Where the Kafka message definitions live inside the kafka-clients JAR.
    private static final String MESSAGE_RESOURCE_DIR = "common/message/";

    public static void main(String[] args) {
        String outputDir = args.length > 0 ? args[0] : OUTPUT_DIR;

        try {
            Map<String, String> messageJsons = loadMessageJsons();
            System.out.println("Found " + messageJsons.size() + " Kafka message definitions on the classpath.");

            Gson gson = new GsonBuilder().setLenient().create();
            List<ApiSpec> specs = new ArrayList<>();
            List<String> skipped = new ArrayList<>();

            // Parse, keep only requests/responses, and inline any commonStructs.
            for (Map.Entry<String, String> entry : new TreeMap<>(messageJsons).entrySet()) {
                try {
                    String cleaned = JsonCommentStripper.stripComments(entry.getValue());
                    ApiSpec spec = gson.fromJson(cleaned, ApiSpec.class);
                    if (spec == null || spec.getType() == null || spec.getName() == null) {
                        continue;
                    }
                    if (!"request".equals(spec.getType()) && !"response".equals(spec.getType())) {
                        continue; // skip header / record (data) definitions
                    }
                    if ("none".equals(spec.getValidVersions())) {
                        continue; // deprecated / unsupported message
                    }
                    CommonStructResolver.resolve(spec);
                    specs.add(spec);
                } catch (Exception e) {
                    skipped.add(entry.getKey() + " (parse: " + e.getMessage() + ")");
                }
            }

            // Generate the Go code and round-trip test for each message. Failures are isolated so one
            // unsupported message cannot abort the whole run.
            // Only specs whose Go code was written successfully may be referenced by the registries;
            // otherwise messages.go could import a package that was never generated.
            List<ApiSpec> generatedSpecs = new ArrayList<>();
            for (ApiSpec spec : specs) {
                try {
                    String goCode = new GoCodeGenerator(spec).generate();
                    String testCode = new GoTestGenerator(spec).generate();

                    Path packageDir = Paths.get(outputDir, TypeUtils.toPackageName(spec.getName()));
                    Files.createDirectories(packageDir);
                    Files.write(packageDir.resolve(spec.getType() + ".go"), goCode.getBytes(StandardCharsets.UTF_8));
                    Files.write(packageDir.resolve(spec.getType() + "_test.go"), testCode.getBytes(StandardCharsets.UTF_8));
                    generatedSpecs.add(spec);
                } catch (Exception e) {
                    skipped.add(spec.getName() + " (generate: " + e.getMessage() + ")");
                }
            }
            int generated = generatedSpecs.size();

            // Generate the apis.go header-version lookup tables from the successfully generated specs.
            try {
                String apisCode = new ApisGenerator(generatedSpecs).generate();
                Path apisDir = Paths.get(outputDir).getParent().resolve("apis");
                Files.createDirectories(apisDir);
                Files.write(apisDir.resolve("apis.go"), apisCode.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                System.err.println("Error generating apis.go: " + e.getMessage());
                e.printStackTrace();
            }

            // Generate the messages.go registry (api key -> body struct, names, version ranges) from
            // all parsed specs. Lives in its own leaf package to avoid an import cycle with protocol.
            try {
                String messagesCode = new MessagesGenerator(generatedSpecs).generate();
                Path messagesDir = Paths.get(outputDir).getParent().resolve("messages");
                Files.createDirectories(messagesDir);
                Files.write(messagesDir.resolve("messages.go"), messagesCode.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                System.err.println("Error generating messages.go: " + e.getMessage());
                e.printStackTrace();
            }

            System.out.println("Generated " + generated + " of " + specs.size() + " message files into " + outputDir);
            if (!skipped.isEmpty()) {
                System.out.println("Skipped " + skipped.size() + ":");
                skipped.forEach(s -> System.out.println("  - " + s));
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /** Reads every {@code common/message/*.json} resource from the kafka-clients JAR on the classpath. */
    private static Map<String, String> loadMessageJsons() throws Exception {
        ClassLoader cl = Generator.class.getClassLoader();
        // Use a stable, always-present message as an anchor to locate the JAR (or directory).
        URL anchor = cl.getResource(MESSAGE_RESOURCE_DIR + "ApiVersionsRequest.json");
        if (anchor == null) {
            throw new IllegalStateException(
                    "Kafka message definitions not found on the classpath - is the kafka-clients dependency present?");
        }

        Map<String, String> jsons = new TreeMap<>();
        if ("jar".equals(anchor.getProtocol())) {
            JarURLConnection connection = (JarURLConnection) anchor.openConnection();
            Path jarPath = Paths.get(connection.getJarFileURL().toURI());
            try (JarFile jar = new JarFile(jarPath.toFile())) {
                Enumeration<JarEntry> entries = jar.entries();
                while (entries.hasMoreElements()) {
                    JarEntry e = entries.nextElement();
                    String name = e.getName();
                    if (name.startsWith(MESSAGE_RESOURCE_DIR) && name.endsWith(".json")) {
                        try (InputStream in = jar.getInputStream(e)) {
                            jsons.put(fileName(name), new String(in.readAllBytes(), StandardCharsets.UTF_8));
                        }
                    }
                }
            }
        } else {
            // Resources on disk (e.g. an exploded classpath): list the containing directory.
            Path dir = Paths.get(anchor.toURI()).getParent();
            try (var paths = Files.list(dir)) {
                for (Path p : (Iterable<Path>) paths::iterator) {
                    if (p.getFileName().toString().endsWith(".json")) {
                        jsons.put(p.getFileName().toString(), Files.readString(p));
                    }
                }
            }
        }
        return jsons;
    }

    private static String fileName(String resourcePath) {
        return resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
    }
}
