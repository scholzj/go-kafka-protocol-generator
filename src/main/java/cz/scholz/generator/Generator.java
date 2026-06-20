package cz.scholz.generator;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.util.JsonCommentStripper;
import cz.scholz.generator.util.TypeUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Generator {
    // Defaults assume the generator is run from the repository root. The output goes into the
    // go-kafka-protocol git submodule; apis.go lands in its sibling "apis" directory.
    private static final String SPEC_DIR = "spec";
    private static final String OUTPUT_DIR = "go-kafka-protocol/api";

    public static void main(String[] args) {
        String specDir = args.length > 0 ? args[0] : SPEC_DIR;
        String outputDir = args.length > 1 ? args[1] : OUTPUT_DIR;

        try {
            File specDirectory = new File(specDir);
            if (!specDirectory.exists() || !specDirectory.isDirectory()) {
                System.err.println("Spec directory does not exist: " + specDir);
                System.exit(1);
            }

            File[] jsonFiles = specDirectory.listFiles((dir, name) -> name.endsWith(".json"));
            if (jsonFiles == null || jsonFiles.length == 0) {
                System.err.println("No JSON files found in: " + specDir);
                System.exit(1);
            }

            Gson gson = new GsonBuilder().setLenient().create();
            List<ApiSpec> allSpecs = new ArrayList<>();

            for (File jsonFile : jsonFiles) {
                System.out.println("Processing: " + jsonFile.getName());
                
                try {
                    String jsonContent = new String(Files.readAllBytes(jsonFile.toPath()));
                    String cleanedJson = JsonCommentStripper.stripComments(jsonContent);
                    
                    ApiSpec spec = gson.fromJson(cleanedJson, ApiSpec.class);

                    if (!"none".equals(spec.getValidVersions())) {
                        allSpecs.add(spec);
                    }
                    
                    //GoCodeGenerator generator = new GoCodeGenerator(spec);
                    //String goCode = generator.generate();
                    //
                    //// Determine output directory based on package name
                    //String packageName = getPackageName(spec.getName());
                    //Path outputPath = Paths.get(outputDir, packageName);
                    //Files.createDirectories(outputPath);
                    //
                    //// Determine file name (request.go or response.go)
                    //String fileName = spec.getType() + ".go";
                    //Path outputFile = outputPath.resolve(fileName);
                    //
                    //Files.write(outputFile, goCode.getBytes());
                    //System.out.println("Generated: " + outputFile);
                    
                } catch (Exception e) {
                    System.err.println("Error processing " + jsonFile.getName() + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // Generate request / response code
            try {
                for (ApiSpec spec : allSpecs) {
                    GoCodeGenerator generator = new GoCodeGenerator(spec);
                    String goCode = generator.generate();

                    // Determine output directory based on package name
                    String packageName = TypeUtils.toPackageName(spec.getName());
                    Path outputPath = Paths.get(outputDir, packageName);
                    Files.createDirectories(outputPath);

                    // Determine file name (request.go or response.go)
                    String fileName = spec.getType() + ".go";
                    Path outputFile = outputPath.resolve(fileName);

                    Files.write(outputFile, goCode.getBytes());
                    System.out.println("Generated: " + outputFile);

                    // Generate a round-trip test alongside the message (request_test.go / response_test.go)
                    String testCode = new GoTestGenerator(spec).generate();
                    Path testFile = outputPath.resolve(spec.getType() + "_test.go");
                    Files.write(testFile, testCode.getBytes());
                    System.out.println("Generated: " + testFile);
                }
            } catch (Exception e) {
                System.err.println("Error generating api package: " + e.getMessage());
                e.printStackTrace();
            }

            // TODO: Valid versions support

            // Generate apis.go file
            try {
                ApisGenerator apisGenerator = new ApisGenerator(allSpecs);
                String apisCode = apisGenerator.generate();
                
                // Determine output directory for apis.go (should be in go/apis directory)
                // Assuming outputDir is ../go/api, apis.go should be in ../go/apis
                Path apisOutputDir = Paths.get(outputDir).getParent().resolve("apis");
                Files.createDirectories(apisOutputDir);
                Path apisOutputFile = apisOutputDir.resolve("apis.go");
                
                Files.write(apisOutputFile, apisCode.getBytes());
                System.out.println("Generated: " + apisOutputFile);
            } catch (Exception e) {
                System.err.println("Error generating apis.go: " + e.getMessage());
                e.printStackTrace();
            }
            
            System.out.println("Generation complete!");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

