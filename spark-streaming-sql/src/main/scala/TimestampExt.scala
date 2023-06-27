import java.security.MessageDigest
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp

object TimestampExt {
  def register(spark: SparkSession): Unit = {
    spark.udf.register("from_epoch_millis", fromEpochMillis(_))
  }

  def fromEpochMillis(in: Option[Long]): Option[Timestamp] = {
    in.map(new Timestamp(_))
  }
}
