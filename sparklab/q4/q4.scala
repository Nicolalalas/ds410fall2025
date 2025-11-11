import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Q4 {
  def main(args: Array[String]): Unit = {
    val sc = getSC()
    val myrdd = getRDD(sc)
    val result = doCities(myrdd)
    saveit("spark4output", result)
  }

  def getSC(): SparkContext = {
    new SparkContext(new SparkConf().setAppName("Q4"))
  }

  def getRDD(sc: SparkContext): RDD[String] = {
    sc.textFile("hdfs:///datasets/cities")
  }

  def doCities(input: RDD[String]): RDD[(String, (Int, Int, Long))] = {
    val header = input.first()
    val headerFields = header.split("\t", -1)
    val nameToIdx = headerFields.zipWithIndex.map { case (n, i) => (n, i) }.toMap
    val iState = nameToIdx("state")
    val iPop   = nameToIdx("population")

    val body = input
      .filter(line => line != header)
      .flatMap { line =>
        val f = line.split("\t", -1)
        if (f.length > math.max(iState, iPop)) {
          val s = f(iState)
          val pStr = f(iPop)
          try {
            val p = pStr.toLong
            if (s.nonEmpty) Some((s, p)) else None
          } catch { case _: Throwable => None }
        } else None
      }

    val stateAgg = body
      .map { case (s, p) =>
        val large = if (p > 100000L) 1 else 0
        (s, (1, large, p))
      }
      .reduceByKey { case ((c1, l1, t1), (c2, l2, t2)) =>
        (c1 + c2, l1 + l2, t1 + t2)
      }

    stateAgg
  }

  def doRetail(input: RDD[String]): RDD[(String, (Int, Int, Long))] = {
    doCities(input)
  }

  def getTestRDD(sc: SparkContext): RDD[String] = {
    sc.parallelize(Seq(
      "name\tstate\tcounty\tpopulation\tzip\tid",
      "Alpha\tPA\tX\t90000\t19000\t1",
      "Beta\tPA\tX\t120000\t19001\t2",
      "Gamma\tDC\tX\t700000\t20001\t3",
      "Bad\tNY\tX\tnotnum\t10001\t4",
      "Delta\tNY\tX\t80000\t10002\t5",
      "Epsilon\tNY\tX\t150000\t10003\t6"
    ))
  }

  def expectedOutput(sc: SparkContext): RDD[(String, (Int, Int, Long))] = {
    sc.parallelize(Seq(
      ("PA", (2, 1, 210000L)),
      ("DC", (1, 1, 700000L)),
      ("NY", (2, 1, 230000L))
    ))
  }

  def saveit(name: String, counts: RDD[(String, (Int, Int, Long))]): Unit = {
    counts.saveAsTextFile(name)
  }
}

