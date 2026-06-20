package cz.scholz.generator.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VersionUtils {
    private static final Pattern VERSION_RANGE = Pattern.compile("(\\d+)(?:-(\\d+))?");
    private static final Pattern VERSION_PLUS = Pattern.compile("(\\d+)\\+");
    private static final Pattern SINGLE_VERSION = Pattern.compile("(\\d+)");

    public static int getStartVersion(String versions) {
        if (versions == null || versions.isEmpty()) {
            throw new RuntimeException("Invalid version: " + versions);
        }

        Matcher singleMatcher = SINGLE_VERSION.matcher(versions);
        if (singleMatcher.matches()) {
            return Integer.parseInt(singleMatcher.group(1));
        }

        Matcher plusMatcher = VERSION_PLUS.matcher(versions);
        if (plusMatcher.matches()) {
            return Integer.parseInt(plusMatcher.group(1));
        }
        
        Matcher rangeMatcher = VERSION_RANGE.matcher(versions);
        if (rangeMatcher.matches()) {
            return Integer.parseInt(rangeMatcher.group(1));
        }
        
        return Integer.MAX_VALUE;
    }

    public static int getEndVersion(String versions) {
        if (versions == null || versions.isEmpty()) {
            throw new RuntimeException("Invalid version: " + versions);
        }

        Matcher singleMatcher = SINGLE_VERSION.matcher(versions);
        if (singleMatcher.matches()) {
            return Integer.parseInt(singleMatcher.group(1));
        }

        Matcher rangeMatcher = VERSION_RANGE.matcher(versions);
        if (rangeMatcher.matches() && rangeMatcher.group(2) != null) {
            return Integer.parseInt(rangeMatcher.group(2));
        }

        // VERSION_PLUS has no end version -> no need to check, just return Integer.MAX_VALUE

        return Integer.MAX_VALUE;
    }

    public static boolean isVersionInRange(String versionSpec, int version) {
        if (versionSpec == null || versionSpec.isEmpty()) {
            return false;
        }
        
        // Handle "N+" format
        Matcher plusMatcher = VERSION_PLUS.matcher(versionSpec);
        if (plusMatcher.matches()) {
            int minVersion = Integer.parseInt(plusMatcher.group(1));
            return version >= minVersion;
        }
        
        // Handle "N-M" or "N" format
        Matcher rangeMatcher = VERSION_RANGE.matcher(versionSpec);
        if (rangeMatcher.matches()) {
            int minVersion = Integer.parseInt(rangeMatcher.group(1));
            String maxVersionStr = rangeMatcher.group(2);
            if (maxVersionStr != null) {
                int maxVersion = Integer.parseInt(maxVersionStr);
                return version >= minVersion && version <= maxVersion;
            } else {
                return version == minVersion;
            }
        }
        
        return false;
    }

    public static int[] parseVersionRange(String versionRange) {
        if (versionRange == null || versionRange.isEmpty()) {
            return new int[]{0, 0};
        }
        
        Matcher rangeMatcher = VERSION_RANGE.matcher(versionRange);
        if (rangeMatcher.matches()) {
            int min = Integer.parseInt(rangeMatcher.group(1));
            String maxStr = rangeMatcher.group(2);
            int max = maxStr != null ? Integer.parseInt(maxStr) : min;
            return new int[]{min, max};
        }
        
        return new int[]{0, 0};
    }
}

