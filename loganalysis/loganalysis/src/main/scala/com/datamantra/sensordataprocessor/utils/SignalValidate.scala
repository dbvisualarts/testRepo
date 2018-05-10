package com.datamantra.sensordataprocessor.utils

/**
  * Created by RHARIKE on 9/5/2017.
  */
class SignalValidate extends LazyLogging {

  val logMgr = new LoggerManager
  val transFunctions = new TransformationFunctions with Serializable

  def validateSignal (eventType:String,signalLine : String) :  Array[String] = {
    logger.info("event Type is : " +eventType + ", Signal Line is : " +signalLine.toString)

    val signalRegex = """,(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"""
    val evTyp = eventType.toUpperCase

    if (signalLine == null || signalLine.length() == 0) {
      logger.error("Signal line is null")
      throw new IllegalArgumentException("Signal must not be null")
    }

    val sigArray = signalLine.split(signalRegex,-1)

  if(evTyp == "VCD" || evTyp == "SE") {
    if(sigArray.size == 3) {
      val ts = sigArray(0)
      val sigName = sigArray(1)
      val sigValue = sigArray(2)

      logger.info("Timestamp is : " +ts + ", signal Name is : " +sigName + ", signal Value is : " +sigValue)

      val newSigName = modifySignalName(evTyp,sigName)

      val parsedSig = Array(ts,newSigName,sigValue)

      logger.info(evTyp + " Event : Validation of the signal Name to check if such a transformation function exists")

      if(!transFunctions.isSignalValid(sigName)) {
        logger.error(evTyp + " Event : No such transformation function found")
        throw new NoSuchMethodException("No transformation function found - Signal [name = " +sigName + ", value = + " +sigValue + ", Timestamp = " +ts + " ]")
      }

      logger.info(evTyp + " Event : Validation of the signal timestamp to check if it is between Jan 2016 and Jan 2027")

      if(ts.toLong < transFunctions.MIN_VALID_TIMESTAMP || ts.toLong > transFunctions.MAX_VALID_TIMESTAMP) {
        logger.error(evTyp + " Event : The given timestamp " + ts + " is invalid")
        throw new IllegalArgumentException("The given timestamp " + ts + " is invalid for - Signal [name = " +sigName + ", value = + " +sigValue + ", Timestamp = " +ts + " ]")
      }

      logger.info("All validations completed successfully")
      parsedSig
    }
    else {
      logger.error("The signal line has more/less than 3 columns and is invalid")
      throw new IllegalArgumentException("Data row has more/less than 3 columns")
    }
  }
  else {
    logger.info("The signal currently being processed is for STF event")
    if(sigArray.size == 4) {
      val ts = sigArray(0)
      val sigName = sigArray(1)
      val sigInput = sigArray(2)
      val sigOutput = sigArray(3)

      logger.info("Timestamp is : " +ts + ", signal Name is : " +sigName + ", signal Input is : " +sigInput + ", signal Output is : " +sigOutput)

      val newSigName = modifySignalName(evTyp,sigName)

      val parsedSig = Array(ts,newSigName,sigInput,sigOutput)

      logger.info(evTyp + " Event : Validation of the signal Name to check if such a transformation function exists")

      if(!transFunctions.isSignalValid(newSigName)) {
        logger.error(evTyp + " Event : No such transformation function found")
        throw new NoSuchMethodException("No transformation function found - Signal [name = " +newSigName + ", input = " +sigInput + ", output = + " +sigOutput + ", Timestamp = " +ts + " ]")
      }

      logger.info(evTyp + " Event : Validation of the signal timestamp to check if it is between Jan 2016 and Jan 2027")

      if(ts.toLong < transFunctions.MIN_VALID_TIMESTAMP || ts.toLong > transFunctions.MAX_VALID_TIMESTAMP) {
        logger.error(evTyp + " Event : The given timestamp " + ts + " is invalid")
        throw new IllegalArgumentException("The given timestamp " + ts + " is invalid for - Signal [name = " +sigName + ", input = " +sigInput + ", output = + " +sigOutput + ", Timestamp = " +ts + " ]")
      }

      logger.info("All validations completed successfully")
      parsedSig
    }
    else {
      logger.error("The signal line has more/less than 4 columns and is invalid")
      throw new IllegalArgumentException("Data row has more/less than 4 columns")
    }
  }
  }

  def modifySignalName(eType:String,sName:String) :String = {
    if(eType == "STF") {
      val newSName = sName.replaceAll("^\"|\"$", "")
      //val pattern = ("(tcp?://)([^:^/]*)(:\\d*)?(.*)?")

      val result = newSName.split(",")(1)
      result
    } else {
      val result = sName.replaceAll("^\"|\"$", "")
      result
    }
  }

}
