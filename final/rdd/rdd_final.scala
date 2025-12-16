
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
        SparkContext.getOrCreate()

    }

    def getRDD(sc:SparkContext): RDD[String] = { 
        sc.textFile("data/city.tsv")
    }

    def doFinal(input: RDD[String]): OutputType = {
        val cityCoountyCounts = input
        .filter(line => !line.startsWith("City\t"))
        .map(_.split("\t"))
        .filter(parts => parts.length == 7)
        .map(parts => ((parts(0).trim, parts(3).trim),1))
        .reduceBykey(_+_)
        .filter { case (_, count) => count % 3 != 0}

        val cityAgg = cityCountyCounts
        .map { case ((city, _), count) => (city, (1, count))}
        .reducceBykey {
            case ((r1, e1), (r2, e2)) =>
              (r1 + r2, e1 + e2)
        }

    cityAgg.filter { case (_, (riddle, enigma)) => riddle != enigma }    
      
    }
   
    def getTestRDD(sc: SparkContext): RDD[String] = {
        sc.parallelize(Seq(
            "CiTY\tX\tX\tCounty\tX\tX\tX",
            "A\tX\tX\tC1\tX\tX\tX",
            "A\tX\tX\tC1\tX\tX\tX",
            "A\tX\tX\tC2\tX\tX\tX"
            "B\tX\tX\tC3\tX\tX\tX"
            ))

    }

    def expectedOutput(sc: SparkContext): OutputType = {
        sc.parallelize(Seq(
            ("A", (2,3))
            ("B", (1,1))
            ))

    }

    def saveit[T](myrdd: RDD[T], name: String) = {
        myrdd.saveAsTextFile(name)
    }

}

