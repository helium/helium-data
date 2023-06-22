import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.sql.SparkSession
import io.delta.implicits._
import scala.io.Source

object Main extends App {
  def createSparkConfFromEnv(): SparkConf = {
    val sparkConf = new SparkConf().setMaster(sys.props.getOrElse("SPARK_MASTER", "local[*]"))
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    val sparkEnvVars = sys.env.filterKeys(_.startsWith("SPARK_"))
    
    sparkEnvVars.foreach { case (key, value) =>
      val confKey = key.toLowerCase.replace('_', '.')
      sparkConf.set(confKey, value)
    }
    
    sparkConf
  }

  def createTablesFromEnv(spark: SparkSession) {
    val sparkEnvVars = sys.env.filterKeys(_.startsWith("TABLE_"))
    
    sparkEnvVars.foreach { case (key, value) =>
      val tableName = key.substring("TABLE_".length).toLowerCase
      val tablePath = value
      spark.readStream.delta(tablePath).createOrReplaceTempView(tableName)
    }
  }

  val config = createSparkConfFromEnv()
  val spark = SparkSession.builder().config(config).getOrCreate()
  B58.register(spark)
  // val ssc = new StreamingContext(config, Seconds(sys.props.getOrElse("SPARK_INTERVAL", "14400").toInt))
  createTablesFromEnv(spark)
  spark.sql(
    Source.fromFile(sys.env.get("QUERY_PATH").get).mkString
  )
    .writeStream
    .format("delta")
    .outputMode("append")
    .partitionBy(sys.env.get("PARTITION_BY").get.split(","): _*)
    .option("checkpointLocation", sys.env.get("CHECKPOINT").get)
    .start(sys.env.get("OUTPUT").get)
    .awaitTermination()
}

