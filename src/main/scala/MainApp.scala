import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

object MainApp extends Logging {
  def main(args: Array[String]): Unit = {
    // Config 파일 로드
    val conf = ConfigFactory.load()

    val appName = conf.getString("spark.name")
    val master = conf.getString("spark.master")
    val inputPath = conf.getString("spark.aws.s3.input_data_path")
    val outputPath = conf.getString("spark.aws.s3.output_data_path")
    val s3MetastorePath = conf.getString("spark.metastore")
    val checkpointDir = conf.getString("spark.checkpoint_dir")  // 체크포인트 디렉토리 설정

    // SparkConf 생성
    val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
    val spark = SparkSession.builder.config(sparkConf)
      .config("spark.sql.catalogImplementation", "hive") // Hive 카탈로그 사용
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") // S3 파일 시스템 설정
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .enableHiveSupport()
      .getOrCreate()

    // 체크포인트 디렉토리 설정
    spark.sparkContext.setCheckpointDir(checkpointDir)

    val dataLoader = new DataLoader(spark)
    val batchProcessor = new BatchProcessor(spark)
    val hiveManager = new HiveManager(spark)

    // FileSystem 객체 생성
//    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // FileSystem 객체 생성 시 Hadoop configuration에서 S3A 파일 시스템을 사용하도록 명시적으로 설정
    val fs = FileSystem.get(new java.net.URI(s3MetastorePath), spark.sparkContext.hadoopConfiguration)


    try {
      // step1 체크포인트 경로 설정
      val checkpointStep1Path = new Path(s"$checkpointDir/step1")
      var userActivityDF = if (fs.exists(checkpointStep1Path)) {
        logInfo(s"step1 체크포인트 발견. 경로: $checkpointStep1Path")
        spark.read.parquet(checkpointStep1Path.toString)
      } else {
        logInfo("step1 체크포인트가 없습니다. 데이터 로드 및 UTC -> KST 처리 시작.")
        val rawDF = dataLoader.loadData(inputPath)
        val transformedDF = batchProcessor.transformStep1(rawDF)
        transformedDF.checkpoint() // lineage 제거
        transformedDF.write.mode("overwrite").parquet(checkpointStep1Path.toString)
        transformedDF
      }

      // step2 체크포인트 경로 설정
      val checkpointStep2Path = new Path(s"$checkpointDir/step2")
      userActivityDF = if (fs.exists(checkpointStep2Path)) {
        logInfo(s"step2 체크포인트 발견. 경로: $checkpointStep2Path")
        spark.read.parquet(checkpointStep2Path.toString)
      } else {
        logInfo("step2 체크포인트가 없습니다. 파티셔닝 작업 시작.")
        val transformedDF = batchProcessor.transformStep2(userActivityDF)
        transformedDF.checkpoint() /// lineage 제거
        transformedDF.write.mode("overwrite").parquet(checkpointStep2Path.toString)
        transformedDF
      }

      // 최종 처리 및 Parquet 저장
      batchProcessor.processBatch(userActivityDF, outputPath)

      // Hive External Table 생성 및 데이터 저장
      hiveManager.createExternalTable("default", "user_activity", outputPath)

    } catch {
      case e: Exception =>
        logError("에러가 발생했습니다. 에러 메시지:", e)
        e.printStackTrace()

    } finally {
      // Spark 컨텍스트 종료
      logInfo("Spark 세션을 종료합니다.")
      spark.stop()

      // JVM 종료
      System.exit(0)
    }
  }
}
