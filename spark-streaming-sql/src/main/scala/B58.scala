import java.security.MessageDigest
import org.apache.spark.sql.SparkSession

object B58 {
  def register(spark: SparkSession) {
    spark.udf.register("b58encode", encodeToBase58 _)
    spark.udf.register("b58encodeChecked", encodeToBase58Checked _)
  }

  val alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
  val idxToChar = Map(alphabet.zipWithIndex.map(_.swap): _*)

  def encodeToBase58Checked(arrayOpt: Option[Array[Byte]]): Option[String] =
    encodeToBase58(arrayOpt.map(array => array.slice(0, 32)))

  def encodeToBase58(arrayOpt: Option[Array[Byte]]): Option[String] =
    arrayOpt.map(array =>
      (LazyList.fill(array.takeWhile(_ == 0).length)(1.toByte) ++ LazyList
        .unfold(
          BigInt(0.toByte +: array)
        )(n => if (n == 0) None else Some((n /% 58).swap))
        .map(_.toInt)
        .reverse
        .map(x => idxToChar(x))).mkString
    )
}
