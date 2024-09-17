import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object HiveMetastoreTest {

  def main(args: Array[String]): Unit = {

    // SparkSession 생성 (Hive 지원 활성화)
    val spark = SparkSession.builder()
      .appName("HiveMetastoreTest")
      .master("local[*]")  // 로컬 모드에서 실행 (모든 CPU 코어 사용)
      .config("spark.sql.warehouse.dir", "s3a://backpacker-assignment-test/hive-warehouse")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .config("spark.sql.hive.convertMetastoreParquet", "false")  // Parquet 변환 비활성화
      .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true") // 디렉토리 내부 파일 재귀적으로 읽기

      .enableHiveSupport()  // Hive 지원 활성화
      .getOrCreate()

    // Hive 테이블 쿼리 테스트
    val dbName = "default"  // 사용할 데이터베이스
    val tableName = "user_activity"  // 쿼리할 테이블 이름

    try {

      spark.sql("SHOW TABLES").show()

      // Spark SQL을 사용하여 테이블 정보를 조회하는 코드
      spark.sql(s"DESCRIBE FORMATTED $tableName").show(100, truncate = false)


      // Hive 테이블에서 데이터 읽기
      val df: DataFrame = spark.sql(s"SELECT * FROM $dbName.$tableName LIMIT 10")

      // 데이터 확인 출력
      df.show()

      // 특정 컬럼을 기준으로 간단한 집계 쿼리
      val countByEventType = df.groupBy("event_type").count()
      countByEventType.show()

    } catch {
      case e: Exception =>
        println(s"Error occurred while querying the Hive metastore: ${e.getMessage}")
    } finally {
      // SparkSession 종료
      spark.stop()
    }
  }
}
