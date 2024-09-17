import org.apache.spark.sql.SparkSession

class HiveManager(spark: SparkSession) {
  def createExternalTable(database: String, tableName: String, outputPath: String): Unit = {
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS $database.$tableName (
        event_time TIMESTAMP,
        event_type STRING,
        product_id BIGINT,
        category_id BIGINT,
        category_code STRING,
        brand STRING,
        price DOUBLE,
        user_id BIGINT,
        user_session STRING
      )
      STORED AS PARQUET
      LOCATION '$outputPath'
    """)
  }
}

