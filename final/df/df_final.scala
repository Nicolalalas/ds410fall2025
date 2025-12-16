object DFFinal {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark)
        val answer = doFinal(mydf)
        saveit(answer, "df_final")

    }

    def doFinal(input: DataFrame): DataFrame = {
        import input.sparkSession.implicits._
        import org.apache.spark.sql.functions._

        val cleaned = input
        .select(
            col("_c0").as("City"),
            col("_c3").as("County")
            )

        val cityCountyCounts = input
        .groupBy($"City",$"County")
        .count()
        .filter($"count" % 3 =!= 0)

        val cityAgg = cityCountyCounts
        .groupBy($"city")
        .agg(
            count($"County").as("riddle"),
            sum($"count").as("enigma")
            )

        cityAgg.filters($"riddle" =!= $"enigma")

    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getDF(spark: SparkSession): DataFrame = {
        spark.read
        .option("header","true")
        .option("sep", "\t")
        .csv("data/city.tsv")

    }
    
    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._

        Seq(
            ("A","C1"),
            ("A","C1"),
            ("A","C2"),
            ("B","C3")
            ). toDF("City", "County")

    }

    def expectedOutput(spark: SparkSession): DataFrame = {
        import spark.implicits._

        Seq(
            ("A", 2L, 3L),
            ("B", 1L, 1L)
            ).toDF("City","riddle", "enigma")

    }
 
    def saveit(counts: DataFrame, name: String) = {
      counts.write.format("csv").mode("overwrite").save(name)

    }

}


