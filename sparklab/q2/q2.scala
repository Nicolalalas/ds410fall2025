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
    val header = lines.take(1)(0)
    val headerFields = header.split("\t", -1)
    val nameToIdx = headerFields.indices.map(i => headerFields(i) -> i).toMap
    val idxInv = nameToIdx("InvoiceNo")
    val idxCus = nameToIdx("CustomerID")

    val body = lines.mapPartitionsWithIndex{ case (pi, it) => if (pi==0) it.drop(1) else it }
      .map{ l => val f = l.split("\t", -1); (f(idxCus), f(idxInv)) }
      .filter{ case (c, inv) => c.nonEmpty && inv.nonEmpty }

    val lineCnt = body.map{ case (c, _inv) => (c, 1) }.reduceByKey((a,b)=>a+b)
    val ordCnt  = body.distinct().map{ case (c, _inv) => (c, 1) }.reduceByKey((a,b)=>a+b)
    lineCnt.join(ordCnt).map{ case (c,(lc,oc)) => (c,(lc,oc)) }
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

  def saveit(name: String, counts: RDD[(String, (Int, Int))]): Unit =
    counts.saveAsTextFile(name)
}

