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
    val header = input.take(1)(0)
    val headerFields = header.split("\t", -1)
    val nameToIdx = headerFields.indices.map(i => headerFields(i) -> i).toMap
    val iInvoice = nameToIdx("InvoiceNo")
    val iCustomer = nameToIdx("CustomerID")
    val iDesc = nameToIdx("Description")

    val body = input.mapPartitionsWithIndex{ case (pi,it) => if (pi==0) it.drop(1) else it }
      .map{ l => val f = l.split("\t", -1); (f(iCustomer), f(iInvoice), f(iDesc)) }
      .filter{ case (c, inv, _) => c.nonEmpty && inv.nonEmpty }

    val lineCount = body.map{ case (c,_inv,_d) => (c,1) }.reduceByKey((a,b)=>a+b)

    val iteOrderCount = body
      .filter{ case (_c,_inv,d) => d.contains("ITE") }
      .map{ case (c, inv, _d) => (c, inv) }
      .distinct()
      .map{ case (c,_inv) => (c,1) }
      .reduceByKey((a,b)=>a+b)

    lineCount.leftOuterJoin(iteOrderCount).map{ case (c,(lc,oc)) => (c,(lc, oc.getOrElse(0))) }
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

