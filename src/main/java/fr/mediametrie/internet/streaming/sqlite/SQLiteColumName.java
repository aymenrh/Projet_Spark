package fr.mediametrie.internet.streaming.sqlite;

import java.io.Serializable;

/**
 * This enum describe all SQLite report column.
 */
public enum SQLiteColumName implements Serializable {

    CMSVI("cmsVI", "varchar (20) NOT NULL default \" \""),
    HASH("hash", "char(33) NOT NULL default \" \""),
    COOKIE("cookie", "varchar (50) NOT NULL default \" \""),
    LIBELLE("libelle", "varchar(256) NOT NULL default \" \""),
    LIBELLE2("libelle2", "varchar(256) NOT NULL default \" \""),
    LIBELLE3("libelle3", "varchar(256) NOT NULL default \" \""),
    LIBELLE4("libelle4", "varchar(256) NOT NULL default \" \""),
    LIBELLE5("libelle5", "varchar(256) NOT NULL default \" \""),
    LIBELLE6("libelle6", "varchar(256) NOT NULL default \" \""),
    LIBELLE7("libelle7", "varchar(256) NOT NULL default \" \""),
    DOMAINE("domaine", "varchar(256) NOT NULL default \" \""),
    GENRE("genre", "varchar(256) NOT NULL default \" \""),
    TERMINAL("terminal", "varchar(200) NOT NULL default \" \""),
    MARQUE("marque", "varchar(200) NOT NULL default \" \""),
    OS("os", "varchar(200) NOT NULL default \" \""),
    OS_VERSION("os_version", "varchar(200) NOT NULL default \" \""),
    IP("IP", "varchar(200) NOT NULL default \" \""),
    UA("UA", "varchar(700) NOT NULL default \" \""),
    MULTILEVEL("multilevel", "varchar(4092) NOT NULL default \" \""),
    TS_VIS("ts_vis", "int(12) NOT NULL"),
    DURATION("duration", "int(20) NOT NULL"),
    MASK1("mask1", "INTEGER NOT NULL"),
    MASK2("mask2", "INTEGER NOT NULL"),
    TS_BEGIN("ts_begin", "int(12) NOT NULL"),
    TS_END("ts_end", "int(12) NOT NULL"),
    VISIT_ID("visit_id", "int(20) NOT NULL"),
    VISITOR_ID("visitor_id", "int(20) NOT NULL"),
    VISUALISATIONS("visualisations", "int(10) NOT NULL"),
    VISUALISATIONS_SIGNIFICATIVES("visualisations_significatives", "int(10) NOT NULL"),
    AUDITEURS_COOKIE("auditeurs_cookie", "int(10) NOT NULL"),
    DUREE("duree", "int(10) NOT NULL"),
    DUREE_MAX("duree_max", "int(10) NOT NULL"),
    AFFICHAGE("affichage", "int(10) NOT NULL"),
    AFFICHAGE_MAX("affichage_max", "int(10) NOT NULL"),
    VISITES("visites", "int(10) NOT NULL"),
    AUDITEURS_NORME("auditeurs_norme", "int(10) NOT NULL"),
    PAYS("pays", "varchar(256) NOT NULL default \" \""),
    VILLE("ville", "varchar(200) NOT NULL default \" \""),
    TAUX_MOYEN_LECTURE("taux_moyen_lecture", "real(10)"),
    TS("ts", "int(12) NOT NULL"),
    COOKIE_INDICATOR("cookie", "int(10) NOT NULL"),
    DUREE_TOTAL("duree_total", "int(10) NOT NULL");

    // Column name into SQLite.
    private final String columnName;
    // Config for create table query.
    private final String columConfig;

    SQLiteColumName(String columnName, String columConfig) {
        this.columnName = columnName;
        this.columConfig = columConfig;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumConfig() {
        return columConfig;
    }
}
