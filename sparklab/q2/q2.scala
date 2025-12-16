<<<<<<< HEAD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q2 {
  def main(args: Array[String]): Unit = {
    val sc = getSC()
    val rdd = getRDD(sc)
    val out = doRetail(sc, rdd)
    saveit("spark2output", out)
  }

  def getSC(): SparkContext = new SparkContext(new SparkConf().setAppName("Q2"))

  def getRDD(sc: SparkContext): RDD[String] = sc.textFile("hdfs:///datasets/retailtab")

  def doRetail(sc: SparkContext, lines: RDD[String]): RDD[(String, (Int, Int))] = {
    val body = lines
      .filter(line => !line.startsWith("InvoiceNo\t"))
      .map { line =>
        val f = line.split("\t", -1)
        val customer = if (f.length > 6) f(6) else ""
        val invoice = if (f.length > 0) f(0) else ""
        (customer, invoice)
      }
      .filter { case (c, inv) => c.nonEmpty && inv.nonEmpty }

    val lineCnt = body.map { case (c, _) => (c, 1) }.reduceByKey((a, b) => a + b)
    val ordCnt = body.distinct().map { case (c, _) => (c, 1) }.reduceByKey((a, b) => a + b)

    lineCnt
      .map { case (c, n) => (c, (n, 0)) }
      .union(ordCnt.map { case (c, n) => (c, (0, n)) })
      .reduceByKey { case ((l1, o1), (l2, o2)) => (l1 + l2, o1 + o2) }
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

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] =
    sc.parallelize(Seq(("13047",(3,2)), ("17850",(3,2))))

  def saveit(name: String, counts: RDD[(String, (Int, Int))]): Unit = {
    counts.saveAsTextFile(name)
  }
}

=======
// don't forget imports

object Q2 {
  def main(args: Array[String]) = {  // this is the entry point to our code
    // don't change this function
    val sc = getSC()  // sc variable for your program, 
    val myrdd = getRDD(sc) //  get the rdd
    val result = doRetail(myrdd)  // main computation
    saveit("result", result)  // save the rdd to HDFS, change the folder name
  }

  def getSC(): SparkContext = { // get the spark context variable
  }

  def getRDD(sc:SparkContext): RDD[String] = { 
  }


  def doRetail(input: RDD[String]): RDD[(String, (Int, Int))] = {
       // this does the bulk of the work. 
  }
  def getTestRDD(sc: SparkContext): RDD[String] = {
  }
  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] = {
  }
  def saveit(name: String, counts: RDD[(String, (Int, Int))]) = { 
    // save the RDD, see wordcount example
  }


}
>>>>>>> af68c05f6cdadb328b79d7976e31363f6080174a
