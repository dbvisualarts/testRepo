package com.datamantra.sensordataprocessor.utils

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException}
import java.util.zip.{GZIPInputStream, GZIPOutputStream, ZipException}

import org.apache.commons.compress.utils.IOUtils

/**
  * The Class CompressionUtil for compressing and uncompressing (Kafka) messages.
  */
class GzipUtil extends LazyLogging {
  /**
    * The Constant LOG.
    */
  //private val LOG = LoggerFactory.getLogger(classOf[GzipUtil])
  /**
    * The buffer size for GZIPInputStream and GZIPOutputStream in Bytes.
    */
  private val BUFFER_SIZE = 65536

  /**
    * Compress.
    *
    * @param content the content
    * @return the byte[]
    * @throws RuntimeException the runtime exception
    */
  @throws[RuntimeException]
  def compress(content: Array[Byte]): Array[Byte] = {
    val byteArrayOutputStream = new ByteArrayOutputStream
    try {
      val gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream, BUFFER_SIZE)
      gzipOutputStream.write(content)
      gzipOutputStream.close()
    } catch {
      case e: IOException =>
        throw new RuntimeException(e)
    }
    byteArrayOutputStream.toByteArray
  }

  /**
    * Decompress.
    *
    * @param contentBytes the content bytes
    * @return the byte[]
    * @throws ZipException the zip exception
    */
  @throws[ZipException]
  def decompress(contentBytes: Array[Byte]): String = {
    val out = new ByteArrayOutputStream
    logger.info("Copying the input stream to out variable in the form of byte array")
    try IOUtils.copy(new GZIPInputStream(new ByteArrayInputStream(contentBytes), BUFFER_SIZE), out)
    catch {
      case e: IOException =>
        logger.error(e.getMessage)
        logger.error(("Decompressing failed with this message: " + contentBytes.toString))
        throw new ZipException("Decompression of the file failed with this message: " + contentBytes.toString)
    }
    logger.info("Decompressed data is :"+out.toString)
    out.toString
  }
}
