const scd = require("../index");

/**
 * Create an SCD table on top of the fake table defined in source_data.sqlx.
 */
const { updates } = scd("source_data_scd", {
  // A unique identifier for rows in the table.
  uniqueKey: "user_id",
  // A field that stores a timestamp or date of when the row was last changed.
  timestamp: "updated_at",
  // The source table to build slowly changing dimensions from.
  source: {
    schema: "dataform_scd",
    name: "source_data",
  },
  // Any tags that will be added to actions.
  tags: ["slowly-changing-dimensions"],
  // Any configuration parameters to apply to the incremental table that will be created.
  incrementalConfig: {
    bigquery: {
      partitionBy: "updated_at",
    },
  },
});

// Additional customization of the created models can be done by using the returned actions objects.
updates.config({
  description: "Updates table for SCD",
});