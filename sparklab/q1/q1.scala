import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

object Q1 {
  def main(args: Array[String]): Unit = {
    val sc = getSC()
    val lines = getRDD(sc)
    val counts = doRetail(sc, lines)
    saveit("spark1output", counts)
  }

  def getSC(): SparkContext = new SparkContext(new SparkConf().setAppName("Q1"))

  def getRDD(sc: SparkContext): RDD[String] = sc.textFile("hdfs:///datasets/retailtab")

  // For each customer, count distinct orders (InvoiceNo)
  def doRetail(sc: SparkContext, lines: RDD[String]): RDD[(String, Int)] = {
    val body = lines
      .filter(line => !line.startsWith("InvoiceNo\t"))
      .map { line =>
        val f = line.split("\t", -1)
        val invoice  = if (f.length > 0) f(0) else ""
        val customer = if (f.length > 6) f(6) else ""
        (customer, invoice)
      }
      .filter { case (_c, inv) => inv.nonEmpty }

    val distinctOrders = body.distinct()
    distinctOrders.map { case (c, _inv) => (c, 1) }.reduceByKey((a, b) => a + b)
  }

  def saveit(name: String, counts: RDD[(String, Int)]): Unit = {
    val fs = FileSystem.get(new Configuration())
    val p = new Path(name)
    if (fs.exists(p)) fs.delete(p, true)
    counts.saveAsTextFile(name)
  }
}

