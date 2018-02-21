package com.zuehlke.hackzurich.service

/**
  * The `LinearRegression` class performs a simple linear regression
  * on an set of `n` data points (`y<sub>i</sub>`, `x<sub>i</sub>`).
  * That is, it fits a straight line `y` = &alpha; + &beta; `x`,
  * (where `y` is the response variable, `x` is the predictor variable,
  * &alpha; is the `y-intercept`, and &beta; is the `slope`)
  * that minimizes the sum of squared residuals of the linear regression model.
  * It also computes associated statistics, including the coefficient of
  * determination `R`<sup>2</sup> and the standard deviation of the
  * estimates for the slope and `y`-intercept.
  *
  * @author Robert Sedgewick
  * @author Kevin Wayne
  *
  *         Converted to Scala by Benedikt Wedenik from https://algs4.cs.princeton.edu/14analysis/LinearRegression.java
  */
class LinearRegression(val x: Seq[Double], val y: Seq[Double]) {

  private var intercept = 0.0
  private var slope = 0.0
  private var r2 = 0.0
  private var svar0 = 0.0
  private var svar1 = 0.0

  /**
    * Performs a linear regression on the data points `(y[i], x[i])`.
    *
    * x the values of the predictor variable
    * y the corresponding values of the response variable
    * IllegalArgumentException if the lengths of the two arrays are not equal
    */
  {
    if (x.lengthCompare(y.length) != 0) throw new IllegalArgumentException("array lengths are not equal")
    val n: Int = x.length

    // first pass
    var sumx = 0.0
    var sumy = 0.0
    var sumx2 = 0.0
    var i = 0
    while (i < n) {
      sumx += x(i)
      sumx2 += x(i) * x(i)
      sumy += y(i)
      i += 1
    }

    val xbar: Double = sumx / n
    val ybar: Double = sumy / n

    // second pass: compute summary statistics
    var xxbar = 0.0
    var yybar = 0.0
    var xybar = 0.0
    i = 0
    while (i < n) {
      xxbar += (x(i) - xbar) * (x(i) - xbar)
      yybar += (y(i) - ybar) * (y(i) - ybar)
      xybar += (x(i) - xbar) * (y(i) - ybar)
      i += 1
    }


    slope = xybar / xxbar
    intercept = ybar - slope * xbar
    // more statistical analysis
    var rss = 0.0
    // residual sum of squares
    var ssr = 0.0
    // regression sum of squares
    i = 0
    while (i < n) {
      val fit = slope * x(i) + intercept
      rss += (fit - y(i)) * (fit - y(i))
      ssr += (fit - ybar) * (fit - ybar)
      i += 1
    }

    val degreesOfFreedom: Int = n - 2
    r2 = ssr / yybar
    val svar: Double = rss / degreesOfFreedom
    svar1 = svar / xxbar
    svar0 = svar / n + xbar * xbar * svar1
  }

  /**
    * Returns the expected response `y` given the value of the predictor
    * variable `x`.
    *
    * @param  x the value of the predictor variable
    * @return the expected response { @code y} given the value of the predictor
    *         variable { @code x}
    */
  def predict(x: Double): Double = slope * x + intercept

  /**
    * Returns the standard error of the estimate for the intercept.
    *
    * @return the standard error of the estimate for the intercept
    */
  def interceptStdErr: Double = Math.sqrt(svar0)

  /**
    * Returns the standard error of the estimate for the slope.
    *
    * @return the standard error of the estimate for the slope
    */
  def slopeStdErr: Double = Math.sqrt(svar1)


  /**
    * Returns a string representation of the simple linear regression model.
    *
    * @return a string representation of the simple linear regression model,
    *         including the best-fit line and the coefficient of determination
    *         `R`2
    */
  override def toString: String = {
    val s = new StringBuilder
    s.append(f"$slope%.2f n + $intercept%.2f")
    s.append(f"  (R^2 = $r2%.3f )")
    s.toString
  }
}