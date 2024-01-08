import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.sql.SparkSession
import io.delta.implicits._
import scala.io.Source
import org.apache.spark.sql.streaming.Trigger

object Main extends App {
  def createSparkConfFromEnv(): SparkConf = {
    var sparkConf = new SparkConf()
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
      )
    val master = sys.props.get("SPARK_MASTER")
    if (master.isDefined) {
      sparkConf = sparkConf.setMaster(master.get)
    }
    val sparkEnvVars = sys.env.filterKeys(_.startsWith("SPARK_"))

    sparkEnvVars.foreach { case (key, value) =>
      val confKey = key.toLowerCase.replace('_', '.')
      sparkConf.set(confKey, value)
    }

    sparkConf
  }

  def createTablesFromEnv(spark: SparkSession, isBatch: Boolean): Unit = {
    val sparkEnvVars = sys.env.filterKeys(_.startsWith("TABLE_"))

    sparkEnvVars.foreach { case (key, value) =>
      val tableName = key.substring("TABLE_".length).toLowerCase
      val tablePath = value
      if (isBatch) {
        spark.read.delta(tablePath).createOrReplaceTempView(tableName)
      } else {
        spark.readStream.delta(tablePath).createOrReplaceTempView(tableName)
      }
    }
  }
  val isBatch = sys.env.get("BATCH_PROCESSING").getOrElse("false").toBoolean
  val config = createSparkConfFromEnv()
  val spark = SparkSession.builder().config(config).getOrCreate()
  val trigger = sys.env.getOrElse("TRIGGER", "ProcessingTime") match {
    case "ProcessingTime" => Trigger.ProcessingTime(sys.env.getOrElse("TRIGGER_INTERVAL", "4 hours"))
    case "AvailableNow"    => Trigger.AvailableNow()
    case "Continuous" =>
      Trigger.Continuous(sys.env.get("TRIGGER_INTERVAL").get)
  }
  B58.register(spark)
  TimestampExt.register(spark)
  createTablesFromEnv(spark, isBatch)
  if (isBatch) {
    spark
      .sql(
        Source.fromFile(sys.env.get("QUERY_PATH").get).mkString
      )
      .write
      .format("delta")
      .mode("overwrite")
      .save(sys.env.get("OUTPUT").get)
  } else {
    spark
      .sql(
        Source.fromFile(sys.env.get("QUERY_PATH").get).mkString
      )
      .writeStream
      .format("delta")
      .outputMode("append")
      .partitionBy(sys.env.get("PARTITION_BY").get.split(","): _*)
      .option("checkpointLocation", sys.env.get("CHECKPOINT").get)
      .option("startingTimestamp", sys.env.getOrElse("STARTING_TIMESTAMP", "2017-01-01"))
      .trigger(trigger)
      .start(sys.env.get("OUTPUT").get)
      .awaitTermination()
  }
}
