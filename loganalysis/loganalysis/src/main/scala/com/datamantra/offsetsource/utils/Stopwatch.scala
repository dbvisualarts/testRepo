package com.datamantra.logs_analyzer.offsetsource.utils

/**
 * Created by hduser on 13/4/17.
 */
class Stopwatch {

  private val start = System.currentTimeMillis()

  override def toString() = (System.currentTimeMillis() - start) + " ms"

}
