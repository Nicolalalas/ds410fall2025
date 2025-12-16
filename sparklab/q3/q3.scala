<<<<<<< HEAD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q3 {
  def main(args: Array[String]): Unit = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val result = doRetail(myrdd)
    saveit("spark3output", result)
  }

  def getSC(): SparkContext = new SparkContext(new SparkConf().setAppName("Q3"))
  def getRDD(sc: SparkContext): RDD[String] = sc.textFile("hdfs:///datasets/retailtab")

def doRetail(input: RDD[String]): RDD[(String, (Int, Int))] = {
  val body = input
    .filter(line => !line.startsWith("InvoiceNo\t"))
    .map { line =>
      val f = line.split("\t", -1)
      val customer = if (f.length > 6) f(6) else ""
      val invoice  = if (f.length > 0) f(0) else ""
      val desc     = if (f.length > 2) f(2) else ""
      (customer, invoice, desc)
    }
    .filter { case (_c, inv, _d) => inv.nonEmpty }

  val lineCount = body
    .map { case (c, _inv, _d) => (c, (1, 0)) }

  val iteOrderCount = body
    .filter { case (_c, _inv, d) => d.contains("ITE") }
    .map { case (c, inv, _d) => (c, inv) }
    .distinct()
    .map { case (c, _inv) => (c, (0, 1)) }

  lineCount
    .union(iteOrderCount)
    .reduceByKey { case ((lc1, oc1), (lc2, oc2)) => (lc1 + lc2, oc1 + oc2) }
}

  def getTestRDD(sc: SparkContext): RDD[String] = sc.parallelize(Seq(
    "InvoiceNo\tStockCode\tDescription\tQuantity\tInvoiceDate\tUnitPrice\tCustomerID\tCountry",
    "10001\tA\tWHITE HEART\t1\tT\t1.0\t17850\tUK",
    "10001\tB\tLAMP\t1\tT\t1.0\t17850\tUK",
    "10002\tC\tKITE\t1\tT\t1.0\t17850\tUK",
    "10003\tD\tBAG\t1\tT\t1.0\t13047\tUK",
    "10003\tE\tHOTTIE COVER\t1\tT\t1.0\t13047\tUK",
    "10004\tF\tMUG\t1\tT\t1.0\t13047\tUK"
  ))

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int))] =
    sc.parallelize(Seq(("13047",(3,1)), ("17850",(3,2))))

  def saveit(name: String, counts: RDD[(String, (Int, Int))]): Unit =
    counts.saveAsTextFile(name)
}

=======
// don't forget imports

object Q3 {
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
