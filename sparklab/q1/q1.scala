import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q1 {
  def main(args: Array[String]): Unit = {
    val sc = getSC()
    val lines = getRDD(sc)
    val counts = doRetail(sc, lines)
    saveit("spark1output", counts)
  }

  def getSC(): SparkContext = {
    new SparkContext(new SparkConf().setAppName("Q1"))
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("hdfs:///datasets/retailtab")
  }

  def doRetail(sc: SparkContext, lines: RDD[String]): RDD[(String, Int)] = {
    val header = lines.first()
    val headerFields = header.split("\t", -1)
    val nameToIdx = headerFields.zipWithIndex.map { case (name, idx) => (name, idx) }.toMap
    val iInvoice = nameToIdx("InvoiceNo")
    val iCustomer = nameToIdx("CustomerID")
    val custInvoice = lines
      .filter(line => line != header)
      .map { line =>
        val f = line.split("\t", -1)
        val customer = f(iCustomer)
        val invoice = f(iInvoice)
        (customer, invoice)
      }
      .filter { case (c, i) => c.nonEmpty && i.nonEmpty }
    val uniqueOrders = custInvoice.distinct()
    val counts = uniqueOrders.map { case (c, _) => (c, 1) }.reduceByKey((a, b) => a + b)
    counts
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    sc.parallelize(Seq(
      "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
      "10001\t85123A\tWHITE HANGING HEART T-LIGHT HOLDER\t6\t12/1/2010 8:26\t2.55\t17850\tUnited Kingdom",
      "10001\t71053\tWHITE METAL LANTERN\t6\t12/1/2010 8:26\t3.39\t17850\tUnited Kingdom",
      "10002\t84406B\tCREAM CUPID HEARTS COAT HANGER\t8\t12/1/2010 8:28\t2.75\t17850\tUnited Kingdom",
      "10003\t84029G\tKNITTED UNION FLAG HOT WATER BOTTLE\t6\t12/1/2010 8:34\t3.39\t13047\tUnited Kingdom",
      "10003\t84029E\tRED WOOLLY HOTTIE WHITE HEART.\t6\t12/1/2010 8:34\t3.39\t13047\tUnited Kingdom",
      "10004\t22752\tSET 7 BABUSHKA NESTING BOXES\t2\t12/1/2010 8:35\t7.65\t13047\tUnited Kingdom"
    ))
  }

  def expectedOutput(sc: SparkContext): RDD[(String, Int)] = {
    sc.parallelize(Seq(("13047", 2), ("17850", 2)))
  }

  def saveit(name: String, counts: RDD[(String, Int)]): Unit = {
    counts.saveAsTextFile(name)
  }
}

