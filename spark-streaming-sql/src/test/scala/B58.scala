import org.scalatest.funspec.AnyFunSpec
import java.util.Base64

class B58Spec extends AnyFunSpec {
  describe("b58 encode") {
    it("should encode correctly") {
      val bytes = Base64.getDecoder().decode("AI4B2MNOkMZkGyMbTG1h8ktdcNlVOhJvwTgMmdkhwmuV")
      val out = B58.encodeToBase58Checked(Some(bytes)).get
      assert(out == "1125YPFi3YP2cPFRCrSnPzC6RCo3NvhFjGbB5DjFqoRZVmE1f7V8")
    }
  }
}
