import java.security.MessageDigest
import org.apache.spark.sql.SparkSession

object B58 {
  def register(spark: SparkSession): Unit = {
    spark.udf.register("b58encodeChecked", encodeToBase58Checked _)
    spark.udf.register("b58encode", encodeToBase58 _)
  }

  val alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
  val idxToChar = Map(alphabet.zipWithIndex.map(_.swap): _*)

  def encodeToBase58Checked(arrayOpt: Option[Array[Byte]]): Option[String] =
    arrayOpt.flatMap(array => {
      val prepended = array.prepended(0.toByte)
      val checksum =
        sha256Hash(sha256Hash(prepended)).take(4)
      encodeToBase58(Some(prepended.concat(checksum)))
    })

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

  def sha256Hash(text: Array[Byte]): Array[Byte] =
    java.security.MessageDigest
      .getInstance("SHA-256")
      .digest(text)

}
