package com.datamantra.sensordataprocessor.utils

/**
  * Created by SUDREGO on 8/1/2017.
  */
class Stopwatch {
  private val start = System.currentTimeMillis()
  override def toString() = (System.currentTimeMillis() - start) + "ms"
}
