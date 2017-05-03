package net.snowflake.spark.snowflake.s3upload;

import java.lang.InterruptedException;
import java.lang.RuntimeException;
import java.lang.Thread;

/**
 * Miscellaneous useful functions.
 */
class Utils {

    /**
     * Lets you avoid dealing with {@code InterruptedException} as a checked exception and ensures
     * that the interrupted status of the thread is not lost.
     */
    public static void throwRuntimeInterruptedException(InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
    }

    /**
     * Shortens the given string to the given length by replacing the middle with ...,
     * unless the string is already short enough or almost short enough in which case it is returned unmodified.
     */
    public static String skipMiddle(String string, int length) {
        int inputLength = string.length();
        if (inputLength < length * 1.1) {
            return string;
        }
        int sideLength = (length - 3) / 2;
        StringBuilder builder = new StringBuilder(length);
        builder.append(string, 0, sideLength);
        builder.append("...");
        builder.append(string, inputLength - sideLength, inputLength);
        return builder.toString();
    }
}
