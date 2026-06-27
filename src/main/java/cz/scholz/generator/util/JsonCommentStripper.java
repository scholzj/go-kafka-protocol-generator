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
        // Tracked across lines so a /* ... */ block spanning multiple lines is stripped correctly.
        boolean inBlockComment = false;

        while ((line = reader.readLine()) != null) {
            StringBuilder cleaned = new StringBuilder();
            char[] chars = line.toCharArray();

            for (int i = 0; i < chars.length; i++) {
                char c = chars[i];

                if (inBlockComment) {
                    // Skip everything until the closing */ (which may be on a later line).
                    if (c == '*' && i + 1 < chars.length && chars[i + 1] == '/') {
                        i++;
                        inBlockComment = false;
                    }
                    continue;
                }

                if (inString) {
                    cleaned.append(c);
                    if (c == '"' && (i == 0 || chars[i - 1] != '\\')) {
                        inString = false;
                    }
                    continue;
                }

                if (c == '"') {
                    inString = true;
                    cleaned.append(c);
                } else if (c == '/' && i + 1 < chars.length && chars[i + 1] == '/') {
                    // Single line comment - skip rest of line
                    break;
                } else if (c == '/' && i + 1 < chars.length && chars[i + 1] == '*') {
                    // Block comment start - skip until the matching */ (possibly on a later line).
                    inBlockComment = true;
                    i++;
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

