import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object TestExternalTable extends Logging {
  def main(args: Array[String]): Unit = {
    // Config 파일 로드
    val conf = ConfigFactory.load()

    val appName = conf.getString("spark.name")
    val master = conf.getString("spark.master")
    val outputPath = conf.getString("spark.aws.s3.output_data_path")
    val s3MetastorePath = conf.getString("spark.metastore")

    // SparkConf 생성 및 Hive Metastore를 S3에 저장하도록 설정
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val spark = SparkSession.builder.config(sparkConf)
      .config("spark.sql.catalogImplementation", "hive") // Hive 카탈로그 사용
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") // S3 파일 시스템 설정
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .config("spark.sql.warehouse.dir", s3MetastorePath) // S3 메타스토어 경로 설정
      .config("spark.driver.bindAddress", "127.0.0.1")
      .enableHiveSupport()
      .getOrCreate()

    val hiveManager = new HiveManager(spark)

    try {
      // Hive 데이터 카탈로그에 External Table 생성 및 데이터 저장
      hiveManager.createExternalTable("default", "user_activity", outputPath)
      logInfo(s"External Table created in Hive Metastore at $s3MetastorePath")

    } catch {
      case e: Exception =>
        logError("Error occurred during Hive External Table creation", e)
    } finally {
      logInfo("Stopping Spark session")
      spark.stop()
      System.exit(0)
    }
  }
}
