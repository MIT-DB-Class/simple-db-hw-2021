
package simpledb.common;

/**
 * Debug is a utility class that wraps println statements and allows
 * more or less command line output to be turned on.
 * <p>
 * Change the value of the DEBUG_LEVEL constant using a system property:
 * simpledb.common.Debug. For example, on the command line, use -Dsimpledb.common.Debug=x,
 * or simply -Dsimpledb.common.Debug to enable it at level 0.
 * The log(level, message, ...) method will print to standard output if the
 * level number is less than or equal to the currently set DEBUG_LEVEL.
 */

public class Debug {
  private static final int DEBUG_LEVEL;
  static {
      String debug = System.getProperty("simpledb.common.Debug");
      if (debug == null) {
          // No system property = disabled
          DEBUG_LEVEL = -1;
      } else if (debug.length() == 0) {
          // Empty property = level 0
          DEBUG_LEVEL = 0;
      } else {
          DEBUG_LEVEL = Integer.parseInt(debug);
      }
  }

  private static final int DEFAULT_LEVEL = 0;

  /** Log message if the log level >= level. Uses printf. */
  public static void log(int level, String message, Object... args) {
    if (isEnabled(level)) {
      System.out.printf(message, args);
      System.out.println();
    }
  }

  /** @return true if level is being logged. */
  public static boolean isEnabled(int level) {
    return level <= DEBUG_LEVEL;
  }

  /** @return true if the default level is being logged. */
  public static boolean isEnabled() {
    return isEnabled(DEFAULT_LEVEL);
  }

  /** Logs message at the default log level. */
  public static void log(String message, Object... args) {
    log(DEFAULT_LEVEL, message, args);
  }
}
