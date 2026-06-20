package cz.scholz.generator;

import cz.scholz.generator.model.ApiSpec;
import cz.scholz.generator.util.VersionUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.*;

public class ApisGenerator {
    private final List<ApiSpec> specs;
    
    public ApisGenerator(List<ApiSpec> specs) {
        this.specs = specs;
    }
    
    public String generate() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        
        pw.println("package apis");
        pw.println();
        pw.println("func RequestHeaderVersion(apiKey int16, apiVersion int16) int16 {");
        pw.println("    switch apiKey {");
        
        // Group specs by API key, prioritizing requests
        Map<Integer, ApiSpec> requestSpecs = new TreeMap<>();
        Map<Integer, ApiSpec> responseSpecs = new TreeMap<>();
        
        for (ApiSpec spec : specs) {
            if (spec.getApiKey() == null) continue;
            
            if ("request".equals(spec.getType())) {
                requestSpecs.put(spec.getApiKey(), spec);
            } else if ("response".equals(spec.getType())) {
                responseSpecs.put(spec.getApiKey(), spec);
            }
        }
        
        // Generate request header version cases
        for (Map.Entry<Integer, ApiSpec> entry : requestSpecs.entrySet()) {
            int apiKey = entry.getKey();
            ApiSpec spec = entry.getValue();
            String apiName = getApiName(spec.getName());
            
            int flexibleVersionStart = VersionUtils.getStartVersion(spec.getFlexibleVersions());
            
            pw.println("    case " + apiKey + ": // " + apiName);
            if (flexibleVersionStart < Integer.MAX_VALUE) {
                pw.println("        if apiVersion >= " + flexibleVersionStart + " {");
                pw.println("            return 2");
                pw.println("        } else {");
                pw.println("            return 1");
                pw.println("        }");
            } else {
                // No flexible versions, always use header 1
                pw.println("        return 1");
            }
        }
        
        pw.println("    default:");
        pw.println("        return 1");
        pw.println("    }");
        pw.println("}");
        pw.println();
        pw.println("func ResponseHeaderVersion(apiKey int16, apiVersion int16) int16 {");
        pw.println("    switch apiKey {");
        
        // Generate response header version cases
        for (Map.Entry<Integer, ApiSpec> entry : responseSpecs.entrySet()) {
            int apiKey = entry.getKey();
            ApiSpec spec = entry.getValue();
            String apiName = getApiName(spec.getName());
            
            // Special case: ApiVersions response always uses header 0
            if (spec.getName().equals("ApiVersionsResponse")) {
                pw.println("    case " + apiKey + ": // " + apiName);
                pw.println("        // Always uses response header 0");
                pw.println("        return 0");
                continue;
            }
            
            int flexibleVersionStart = VersionUtils.getStartVersion(spec.getFlexibleVersions());
            
            pw.println("    case " + apiKey + ": // " + apiName);
            if (flexibleVersionStart < Integer.MAX_VALUE) {
                pw.println("        if apiVersion >= " + flexibleVersionStart + " {");
                pw.println("            return 1");
                pw.println("        } else {");
                pw.println("            return 0");
                pw.println("        }");
            } else {
                // No flexible versions, always use header 0
                pw.println("        return 0");
            }
        }
        
        pw.println("    default:");
        pw.println("        return 0");
        pw.println("    }");
        pw.println("}");
        
        return sw.toString();
    }
    
    private String getApiName(String structName) {
        // Convert ApiVersionsRequest -> ApiVersions, MetadataResponse -> Metadata
        return structName.replace("Request", "").replace("Response", "");
    }
}

