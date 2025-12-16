
object RDDFinal {  
    type OutputType = RDD[(String, (Int, Int))]

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, RDDs only
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val answer = doFinal(myrdd) // additional functions to do the computation
        saveit(answer, "rdd_final")  // save the rdd to your home directory in HDFS
    }

    def getSC(): SparkContext = {

    }

    def getRDD(sc:SparkContext): RDD[String] = { // get the orders rdd
    }

    def doFinal(input: RDD[String]): OutputType = { // see definition of OutputType above
       // don't forget: in scala, if you divide two ints, you get an int back
       // if you want a double, you have to convert one of the ints to a double before dividing

    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {

    }

    def expectedOutput(sc: SparkContext): OutputType = { // see definition of OutputType above

    }

    def saveit[T](myrdd: RDD[T], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

