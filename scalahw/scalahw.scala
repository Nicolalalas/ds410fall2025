case class Neumaier(sum: Double, c: Double)

object HW {

    // note it specifies the input (n) and its type (Int) along with the output
    // type List[Int] ( a list of integers)
    def q1(n: Int):List[Int] = {
        List.tabulate(n)(i => (i + 1) * (i + 1))
    }

    // In order to get the code to compile, you have to do the same with the rest of the
    // questions, and then fill in code to make them correct.

    def q2(n: Int): Vector[Double] = {
    Vector.tabulate(n)(i => math.sqrt(i + 1))
    }

    def q3(x: Seq[Double]): Double = {
      x.foldLeft(0.0)(_ + _)
    }

    // you fill in the rest

    def q4(x: Seq[Double]): Double = {
      x.foldLeft(1.0)(_ * _)
    }

    def q5(x: Seq[Double]): Double = {
      x.foldLeft(0.0)((acc, v) => acc + math.log(v))
    }

    
    def q6(x: Seq[(Double, Double)]): (Double, Double) = {
      x.foldLeft((0.0, 0.0)) { case ((sum1, sum2), (a, b)) =>
    (sum1 + a, sum2 + b)
    }
  }

    
    def q7(x: Seq[(Double, Double)]): (Double, Double) = {
      x.foldLeft((0.0, Double.NegativeInfinity)) { case ((sum, maxVal), (a, b)) =>
    (sum + a, math.max(maxVal, b))
    }
  }

    def q8(n: Int): (Int, Int) = {
      val nums = (1 to n).toList
      nums.foldLeft((0, 1)) { case ((sum, prod), x) =>
    (sum + x, prod * x)
    }
  }

    def q9(x: Seq[Int]): Int = {
     x.filter(_ % 2 == 0)
      .map(n => n * n)
      .foldLeft(0)(_ + _)
    }

    def q10(x: Seq[Double]): Double = {
      x.foldLeft((1, 0.0)) { case ((i, acc), value) =>
      (i + 1, acc + value * i)
     }._2
    }


def q11(x: Seq[Double]): Double = {
  val result = x.foldLeft(Neumaier(0.0, 0.0)) { case (state, value) =>
    val t = state.sum + value
    val cNew =
      if (math.abs(state.sum) >= math.abs(value))
        state.c + (state.sum - t) + value
      else
        state.c + (value - t) + state.sum
    Neumaier(t, cNew)
  }
  result.sum + result.c
}
}
