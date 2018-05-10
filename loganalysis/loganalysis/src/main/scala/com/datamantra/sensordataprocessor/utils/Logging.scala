package com.datamantra.sensordataprocessor.utils

import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by SUDREGO on 8/1/2017.
  */

/**
  * Base Logging trait
  */
trait Logging {
  protected  def logger: Logger
}

/**
  * Adds the lazy val `logger` of to the class into which this trait is mixed.
  */
trait LazyLogging extends Logging {
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}

/**
  * Adds the non-lazy val `logger` to the class into which this trait is mixed.
  */
trait StrictLogging extends Logging {
  protected val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}