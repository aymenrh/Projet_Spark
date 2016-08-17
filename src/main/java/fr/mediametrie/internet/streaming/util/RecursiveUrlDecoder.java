package fr.mediametrie.internet.streaming.util;

import java.util.regex.Pattern;

/**
 * Recursive URL decoder
 */
public class RecursiveUrlDecoder {

    private static final Pattern ENCODED_CHAR_PATTERN = Pattern.compile("%[0-9a-fA-F]{2}");

    /**
     * Url decode (recursive : As long as there is an encoded character in the text we decode again)
     *
     * @param input Text to decode
     * @return The text decoded (or the original input if we failed to decode)
     */
    public static String decodeText(String input) {

        if (input == null) {
            return null;
        }

        StringBuilder outputBuilder = new StringBuilder();
        for (int i = 0; i < input.length(); ++i) {
            char c = input.charAt(i);
            if (c == '+') {
                outputBuilder.append(' ');
            } else if (c == '%') {
                if (i + 2 < input.length()) {
                    char c1 = input.charAt(i + 1);
                    char c2 = input.charAt(i + 2);
                    if (isHexa(c1) && isHexa(c2)) {
                        i += 2;
                        outputBuilder.append((char) decode(Character.toLowerCase(c1), Character.toLowerCase(c2)));
                        continue;
                    }
                }
                outputBuilder.append('%');
            } else {
                outputBuilder.append(c);
            }
        }

        String output = outputBuilder.toString();

        if (!ENCODED_CHAR_PATTERN.matcher(output).find()) {
            if (isUTF8(output)) {
                output = utf8ToIso(output);
            }
            return output;
        }

        return RecursiveUrlDecoder.decodeText(output);
    }

    private static boolean isUTF8(String input) {
        char prevC = Character.MIN_VALUE;
        int ok = 0, ko = 0;

        // Check if input string seems to contain UTF-8 encoded character decoded in ISO.
        // For exmaple : é is encoded %c3%a9 in UTF-8 which would have been interpreted as the 2 latin characters : Â and ©
        // during the decodeText method. So presence of Â© would be interpreted as UTF-8 as this point.
        for (int i = 0; i < input.length(); ++i) {
            // Mask each char with 0xC0 (1100000) to check if the char start with 10
            // In this case it can be a UTF8 encoding (check next char)
            char curC = input.charAt(i);
            if ((curC & 0xC0) == 0x80) {
                if ((prevC & 0xC0) == 0xC0) {
                    ok++;
                } else {
                    ko++;
                }
            } else {
                if ((prevC & 0xC0) == 0xC0) {
                    ko++;
                }
            }
            prevC = curC;
        }
        // It looks like a probabilist method but it is reliable when it is called by decodeText
        // But empiric
        return ok > ko;
    }

    private static String utf8ToIso(String utf8Str) {
        // Convert consecutive extra latin characters decoded with ISO to a single character encoded with UTF-8.
        // Basic latin character remains unchanged.
        // exemple : é is UTF-8 endoded %C3%A9 and will be decoded in ISO with 2 extra latin characters Â and ©
        // So presence of "Â©" will be converted to the correct "é".
        StringBuilder builder = new StringBuilder();
        char c, c2, c3, r;
        for (int i = 0; i < utf8Str.length(); ++i) {
            c = utf8Str.charAt(i);
            if (c < 128) {
                // Standard ascii char : Just copy
                builder.append(c);
            } else if (c > 191 && c < 224) {
                //
                if (i + 1 >= utf8Str.length()) {
                    builder.append(c);
                    break;
                }
                c2 = utf8Str.charAt(++i);
                r = (char) (((c & 31) << 6) | (c2 & 63));
                builder.append(r);
            } else if ((c > 223) && (c < 240)) {
                if (i + 1 >= utf8Str.length()) {
                    builder.append(c);
                    break;
                }
                c2 = utf8Str.charAt(++i);
                if (i + 1 >= utf8Str.length()) {
                    builder.append(c2);
                    break;
                }
                c3 = utf8Str.charAt(++i);
                r = (char) (((c & 15) << 12) | ((c2 & 63) << 6) | (c3 & 63));
                builder.append(r);
            } else {
                builder.append('?');
                i += 3;
            }
        }
        return builder.toString();
    }

    private static int decode(char c1, char c2) {
        // Assume that all char are lower case at this point
        return ((c1 >= '0' && c1 <= '9' ? c1 - '0' : c1 - 'a' + 10) * 16)
                + (c2 >= '0' && c2 <= '9' ? c2 - '0' : c2 - 'a' + 10);
    }

    private static boolean isHexa(char c) {
        // Check C is either a digit or a character between 'a' and 'f' (lower or upper)
        return (((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f')));
    }
}