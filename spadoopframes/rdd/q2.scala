import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {  
    type OutputType = RDD[(String, (Long, Long, Long))]

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val counts = doCity(myrdd) // additional functions to do the computation
        saveit(counts, "spadoopframes_q2")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {

    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the big data rdd
         
    }

    def doCity(input: RDD[String]): OutputType = { // see definition of OutputType above
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {
    }

    def expectedOutput(sc: SparkContext): OutputType = { // see definition of OutputType above
    }

    def saveit[T](myrdd: RDD[T], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

