package fr.mediametrie.internet.streaming.sql.query;

/**
 * Common aggregate fields for visualizations
 */
public interface VisuAggregates {

    String VISUALIZATION_COUNT = "COUNT(cmsVi)";
    String VISUALIZATION_SIGNIFICANT_COUNT = "SUM(IF(consumptionDuration > 0, 1, 0))";
    String COOKIE_COUNT = "COUNT(DISTINCT (IF ((cookie != \"\" AND cookie != \"-\"), cookie, NULL)))";
    String COOKIE_SIGNIFICANT = "IF (consumptionDuration > 0 AND cookie != \"\" AND cookie != \"-\", cookie, NULL)";
    String COOKIE_SIGNIFICANT_COUNT = "COUNT(DISTINCT (IF ((consumptionDuration > 0 AND cookie != \"\" AND cookie != \"-\"), cookie, NULL)))";
    String DURATION_SUM = "SUM(consumptionDuration)";
    String DURATION_SIGNIFICANT_SUM = "SUM(IF(consumptionDuration > 0, consumptionDuration, 0))";
    String DURATION_SIGNIFICANT_MAX = "MAX(IF(consumptionDuration > 0, consumptionDuration, 0))";
    String DISPLAY_SUM = "SUM(endTimestamp - beginTimestamp)";
    String DISPLAY_MAX = "MAX(endTimestamp - beginTimestamp)";
    String VISIT_ID_SIGNIFICANT_COUNT = "COUNT(DISTINCT (IF (consumptionDuration > 0, visitId, NULL)))";
    String VISIT_ID_SIGNIFICANT_SET = "collect_set(IF(consumptionDuration > 0, visitId, NULL))";
    String VISITOR_ID_SIGNIFICANT = "IF (consumptionDuration > 0, visitorId, NULL)";
    String VISITOR_ID_SIGNIFICANT_COUNT = "COUNT(DISTINCT (IF (consumptionDuration > 0, visitorId,NULL)))";
    String MASK1 = "binary_or(mask1)";
    String MASK2 = "binary_or(mask2)";

}
