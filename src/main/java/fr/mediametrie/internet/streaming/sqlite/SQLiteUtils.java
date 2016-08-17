package fr.mediametrie.internet.streaming.sqlite;

/**
 * Provides methods related to SQLite db and files.
 */
public class SQLiteUtils {

    public static final String SQLITE_FILE_NAME_PREFIX = "streaminglive_";

    /**
     * Generate the file name to use for the SQLite DB file related to the given serial and date
     *
     * @param date a date using format 'yyyyMMdd'. 'yyyy-MM-dd' is accepted as the '-' are removed.
     */
    public static String generateSQLiteFileName(String serial, String date) {
        return SQLITE_FILE_NAME_PREFIX + serial + "_" + date.replaceAll("-", "");
    }
}
