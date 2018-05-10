package com.datamantra.logs_analyzer.utils

/**
 * Handler for all external write systems
 * @author ashrith
 */
trait Handler {
  /**
   * Closes the stream
   */
  def close()
}
