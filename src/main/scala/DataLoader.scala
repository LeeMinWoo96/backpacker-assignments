import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, LongType, TimestampType}
import org.apache.spark.sql.functions._

class DataLoader(spark: SparkSession) {

  def loadData(s3Path: String): DataFrame = {
    // 스키마 정의
    val schema = StructType(List(
      StructField("event_time", TimestampType, nullable = true),
      StructField("event_type", StringType, nullable = true),
      StructField("product_id", LongType, nullable = true),
      StructField("category_id", LongType, nullable = true),
      StructField("category_code", StringType, nullable = true),
      StructField("brand", StringType, nullable = true),
      StructField("price", DoubleType, nullable = true),
      StructField("user_id", LongType, nullable = true),
      StructField("user_session", StringType, nullable = true)
    ))

    // S3 경로에서 모든 CSV 파일 로드
    val unifiedDF = spark.read
      .schema(schema)
      .csv(s"$s3Path/*.csv")

    unifiedDF
  }
}
