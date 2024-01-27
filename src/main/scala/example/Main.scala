package example
object Main {
  def main(args: Array[String]): Unit = {
    val ages = Seq(42, 75, 29, 64)
    println(s"The oldest person is ${ages.max}")
  }
}
