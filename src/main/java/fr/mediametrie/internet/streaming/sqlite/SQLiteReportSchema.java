package fr.mediametrie.internet.streaming.sqlite;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * This enum describe all SQLite report tables (name and column).
 */
public enum SQLiteReportSchema implements Serializable {

    SYNTHESE("streaming_synthese",
            Arrays.asList(SQLiteColumName.VISUALISATIONS, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.AUDITEURS_COOKIE, SQLiteColumName.DUREE, SQLiteColumName.DUREE_MAX,
                    SQLiteColumName.AFFICHAGE, SQLiteColumName.AFFICHAGE_MAX, SQLiteColumName.VISITES,
                    SQLiteColumName.AUDITEURS_NORME)),
    VISU("streaming_visu",
            Arrays.asList(SQLiteColumName.CMSVI, SQLiteColumName.HASH, SQLiteColumName.COOKIE,
                    SQLiteColumName.LIBELLE, SQLiteColumName.LIBELLE2, SQLiteColumName.LIBELLE3,
                    SQLiteColumName.LIBELLE4, SQLiteColumName.LIBELLE5, SQLiteColumName.LIBELLE6,
                    SQLiteColumName.LIBELLE7, SQLiteColumName.DOMAINE, SQLiteColumName.GENRE),
            Arrays.asList(SQLiteColumName.TERMINAL, SQLiteColumName.MARQUE, SQLiteColumName.OS,
                    SQLiteColumName.OS_VERSION, SQLiteColumName.IP, SQLiteColumName.UA,
                    SQLiteColumName.MULTILEVEL, SQLiteColumName.TS_VIS, SQLiteColumName.DURATION,
                    SQLiteColumName.MASK1, SQLiteColumName.MASK2, SQLiteColumName.TS_BEGIN,
                    SQLiteColumName.TS_END, SQLiteColumName.VISIT_ID, SQLiteColumName.VISITOR_ID)),
    ATTR("streaming_attr",
            Arrays.asList(SQLiteColumName.CMSVI, SQLiteColumName.HASH, SQLiteColumName.PAYS, SQLiteColumName.VILLE),
            Arrays.asList(SQLiteColumName.IP)),
    NOM("streaming_nom",
            Arrays.asList(SQLiteColumName.LIBELLE, SQLiteColumName.LIBELLE2, SQLiteColumName.LIBELLE3,
                    SQLiteColumName.LIBELLE4, SQLiteColumName.LIBELLE5, SQLiteColumName.LIBELLE6,
                    SQLiteColumName.LIBELLE7, SQLiteColumName.DOMAINE, SQLiteColumName.GENRE),
            Arrays.asList(SQLiteColumName.VISUALISATIONS, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.AUDITEURS_COOKIE, SQLiteColumName.DUREE, SQLiteColumName.DUREE_MAX,
                    SQLiteColumName.AFFICHAGE, SQLiteColumName.AFFICHAGE_MAX, SQLiteColumName.VISITES,
                    SQLiteColumName.TAUX_MOYEN_LECTURE, SQLiteColumName.AUDITEURS_NORME)),
    NOM_PAR_MINUTE("streaming_nom_par_minute",
            Arrays.asList(SQLiteColumName.LIBELLE, SQLiteColumName.LIBELLE2, SQLiteColumName.LIBELLE3,
                    SQLiteColumName.LIBELLE4, SQLiteColumName.LIBELLE5, SQLiteColumName.LIBELLE6,
                    SQLiteColumName.LIBELLE7, SQLiteColumName.DOMAINE, SQLiteColumName.GENRE, SQLiteColumName.TS),
            Arrays.asList(SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES, SQLiteColumName.AUDITEURS_COOKIE)),
    PAYS("streaming_par_pays",
            Arrays.asList(SQLiteColumName.PAYS),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS,
                    SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES, SQLiteColumName.DUREE_TOTAL,
                    SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    NOM_PAR_PAYS("streaming_nom_par_pays",
            Arrays.asList(SQLiteColumName.LIBELLE, SQLiteColumName.LIBELLE2, SQLiteColumName.LIBELLE3,
                    SQLiteColumName.LIBELLE4, SQLiteColumName.LIBELLE5, SQLiteColumName.LIBELLE6,
                    SQLiteColumName.PAYS, SQLiteColumName.DOMAINE, SQLiteColumName.GENRE),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR,
                    SQLiteColumName.VISUALISATIONS, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME,
                    SQLiteColumName.TAUX_MOYEN_LECTURE)),
    TERMINAL("streaming_par_terminal",
            Arrays.asList(SQLiteColumName.TERMINAL, SQLiteColumName.OS, SQLiteColumName.OS_VERSION),
            Arrays.asList(SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES, SQLiteColumName.AUDITEURS_COOKIE,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISUALISATIONS, SQLiteColumName.VISITES,
                    SQLiteColumName.AUDITEURS_NORME)),
    NOM_PAR_TERMINAL("streaming_nom_par_terminal",
            Arrays.asList(SQLiteColumName.TERMINAL, SQLiteColumName.OS, SQLiteColumName.OS_VERSION,
                    SQLiteColumName.LIBELLE, SQLiteColumName.LIBELLE2, SQLiteColumName.LIBELLE3,
                    SQLiteColumName.LIBELLE4, SQLiteColumName.LIBELLE5, SQLiteColumName.LIBELLE6,
                    SQLiteColumName.PAYS, SQLiteColumName.DOMAINE, SQLiteColumName.GENRE),
            Arrays.asList(SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES, SQLiteColumName.AUDITEURS_COOKIE,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISUALISATIONS, SQLiteColumName.VISITES,
                    SQLiteColumName.AUDITEURS_NORME, SQLiteColumName.TAUX_MOYEN_LECTURE)),
    LIBELLE("streaming_par_libelle",
            Arrays.asList(SQLiteColumName.LIBELLE),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    LIBELLE2("streaming_par_libelle2",
            Arrays.asList(SQLiteColumName.LIBELLE2),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    LIBELLE3("streaming_par_libelle3",
            Arrays.asList(SQLiteColumName.LIBELLE3),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    LIBELLE4("streaming_par_libelle4",
            Arrays.asList(SQLiteColumName.LIBELLE4),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    LIBELLE5("streaming_par_libelle5",
            Arrays.asList(SQLiteColumName.LIBELLE5),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    LIBELLE6("streaming_par_libelle6",
            Arrays.asList(SQLiteColumName.LIBELLE6),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    GENRE("streaming_par_genre",
            Arrays.asList(SQLiteColumName.GENRE),
            Arrays.asList(SQLiteColumName.COOKIE_INDICATOR, SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES,
                    SQLiteColumName.DUREE_TOTAL, SQLiteColumName.VISITES, SQLiteColumName.AUDITEURS_NORME)),
    MINUTE("streaming_par_minute",
            Arrays.asList(SQLiteColumName.TS),
            Arrays.asList(SQLiteColumName.VISUALISATIONS_SIGNIFICATIVES, SQLiteColumName.AUDITEURS_COOKIE));

    public static final int REPORT_USER_VERSION = 11;

    // Membres :
    private final String tableName;
    private final List<SQLiteColumName> keys;
    private final List<SQLiteColumName> indicators;

    SQLiteReportSchema(String tableName, List<SQLiteColumName> indicators) {
        this(tableName, Lists.newArrayList(), indicators);
    }

    SQLiteReportSchema(String tableName, List<SQLiteColumName> keys, List<SQLiteColumName> indicators) {
        this.tableName = tableName;
        this.keys = keys;
        this.indicators = indicators;
    }

    public String getTableName() {
        return tableName;
    }

    /**
     * Getter of all columns as an ordered Set
     *
     * @return The set containing all the columns of the table.
     */
    public Set<SQLiteColumName> getColumns() {
        Set<SQLiteColumName> columns = Sets.newLinkedHashSet(keys);
        columns.addAll(indicators);
        return columns;
    }

    /**
     * Getter of non-calculated columns.
     *
     * @returns The list of non-claculated columns of the table
     */
    public List<SQLiteColumName> getKeys() {
        return keys;
    }

    /**
     * Getter of calculated columns.
     *
     * @returns The list of calculated columns of the table
     */
    public List<SQLiteColumName> getIndicators() {
        return indicators;
    }

    public static SQLiteReportSchema getSchemaByTableName(String tableName) {
        for (SQLiteReportSchema schema : values()) {
            if (schema.getTableName().equals(tableName)) {
                return schema;
            }
        }
        return null;
    }

    /**
     * Get a SQL create query for a specific table
     *
     * @param table The table to create
     * @return The SQL query to create the table
     */
    public static String getCreateQuery(SQLiteReportSchema table) {
        StringBuilder buffer = new StringBuilder("CREATE TABLE ")
                .append(table.getTableName())
                .append(" ( ")
                .append(table.getColumns().stream()
                        .map(column -> column.getColumnName() + " " + column.getColumConfig())
                        .collect(Collectors.joining(", ")))
                .append(");");
        return buffer.toString();
    }

    /**
     * Get a generic prepared SQL insert query for a specific table.
     * NOTE : This insert includes ALL columns( in the order given by the getColumns method
     *
     * @param table The table where to insert data
     * @return The Sql Query for insert (for prepared statement)
     */
    public static String getInsertQuery(SQLiteReportSchema table) {

        Set<SQLiteColumName> columns = table.getColumns();

        StringBuilder insertQuery = new StringBuilder("INSERT INTO ")
                .append(table.getTableName())
                .append(" ( ")
                .append(columns.stream().map(SQLiteColumName::getColumnName).collect(Collectors.joining(", ")))
                .append(") VALUES (")
                .append(Strings.repeat("?,", columns.size() - 1))
                .append("?)");
        return insertQuery.toString();
    }

    /**
     * Generate unique index query if applicable (at least one column)
     *
     * @param table table info (name and columns)
     * @return return query if applicable, null otherwise
     */
    public static String getCreateIndexUniqueQuery(SQLiteReportSchema table) {
        if (table.getKeys().isEmpty()) {
            return null;
        }
        StringBuilder query = new StringBuilder("CREATE UNIQUE INDEX ")
                .append(table.getTableName())
                .append("_unique ON ")
                .append(table.getTableName())
                .append(" (")
                .append(table.getKeys().stream().map(SQLiteColumName::getColumnName).collect(Collectors.joining(", ")))
                .append(")");
        return query.toString();
    }
}
