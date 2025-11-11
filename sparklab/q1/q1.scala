import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q1 {
  def main(args: Array[String]): Unit = {
    val sc = getSC()
    val lines = getRDD(sc)
    val counts = doRetail(sc, lines)
    saveit("spark1output", counts)
  }

  def getSC(): SparkContext = new SparkContext(new SparkConf().setAppName("Q1"))

  def getRDD(sc: SparkContext): RDD[String] = sc.textFile("hdfs:///datasets/retailtab")

  def doRetail(sc: SparkContext, lines: RDD[String]): RDD[(String, Int)] = {
    val pairs = lines
      .map { line =>
        val f = line.split("\t", -1)
        val invoice = if (f.length > 0) f(0) else ""
        val customer = if (f.length > 6) f(6) else ""
        (customer, invoice)
      }
      .filter { case (_c, inv) => inv.nonEmpty && inv != "InvoiceNo" }

    val distinctOrders = pairs.distinct()
    distinctOrders.map { case (c, _) => (c, 1) }.reduceByKey((a, b) => a + b)
  }

  def getTestRDD(sc: SparkContext): RDD[String] = sc.parallelize(Seq(
    "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
    "10001\tA\tX\t1\tT\t1.0\t17850\tUK",
    "10001\tB\tX\t1\tT\t1.0\t17850\tUK",
    "10002\tC\tX\t1\tT\t1.0\t17850\tUK",
    "10003\tD\tX\t1\tT\t1.0\t13047\tUK",
    "10003\tE\tX\t1\tT\t1.0\t13047\tUK",
    "10004\tF\tX\t1\tT\t1.0\t13047\tUK"
  ))

  def expectedOutput(sc: SparkContext): RDD[(String, Int)] =
    sc.parallelize(Seq(("13047", 2), ("17850", 2)))

  def saveit(name: String, counts: RDD[(String, Int)]): Unit = {
    counts.saveAsTextFile(name)
  }
}

