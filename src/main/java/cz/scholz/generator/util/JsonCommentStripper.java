package cz.scholz.generator.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;

public class JsonCommentStripper {
    public static String stripComments(String json) throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(json));
        StringWriter writer = new StringWriter();
        String line;
        boolean inString = false;
        
        while ((line = reader.readLine()) != null) {
            StringBuilder cleaned = new StringBuilder();
            char[] chars = line.toCharArray();
            
            for (int i = 0; i < chars.length; i++) {
                char c = chars[i];
                
                if (c == '"' && (i == 0 || chars[i - 1] != '\\')) {
                    inString = !inString;
                    cleaned.append(c);
                } else if (!inString && c == '/' && i + 1 < chars.length) {
                    if (chars[i + 1] == '/') {
                        // Single line comment - skip rest of line
                        break;
                    } else if (chars[i + 1] == '*') {
                        // Multi-line comment start - skip until */
                        i++;
                        while (i + 1 < chars.length) {
                            if (chars[i] == '*' && chars[i + 1] == '/') {
                                i++;
                                break;
                            }
                            i++;
                        }
                        continue;
                    } else {
                        cleaned.append(c);
                    }
                } else {
                    cleaned.append(c);
                }
            }
            
            String cleanedLine = cleaned.toString().trim();
            if (!cleanedLine.isEmpty()) {
                writer.write(cleanedLine);
                writer.write('\n');
            }
        }
        
        return writer.toString();
    }
}

