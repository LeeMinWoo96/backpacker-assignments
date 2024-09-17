import org.apache.spark.sql.functions.{col, dayofmonth, from_utc_timestamp, month, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class BatchProcessor(spark: SparkSession) {
  def transformStep1(dataFrame: DataFrame): DataFrame = {
    // UTC 시간 -> KST 시간으로 변환
    val kstDF = dataFrame.withColumn("event_time_kst",
      from_utc_timestamp(col("event_time"), "Asia/Seoul"))

    // 변환된 데이터 반환
    kstDF
  }


  def transformStep2(dataFrame: DataFrame): DataFrame = {
    // KST 시간 기반으로 연도, 월, 일 파티션 추가
    val partitionedDF = dataFrame
      .withColumn("year", year(col("event_time_kst")))
      .withColumn("month", month(col("event_time_kst")))
      .withColumn("day", dayofmonth(col("event_time_kst")))

    // 파티션된 데이터 반환
    partitionedDF
  }

  // Parquet 및 Snappy 압축 저장
  def processBatch(dataFrame: DataFrame, outputPath: String): Unit = {
    dataFrame.write
      .partitionBy("year", "month", "day")
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)
  }
}
