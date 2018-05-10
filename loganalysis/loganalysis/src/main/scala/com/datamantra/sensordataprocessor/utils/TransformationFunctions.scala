package com.datamantra.sensordataprocessor.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.joda.time.DateTime

import scala.collection.mutable.{Map => MutableMap}

/**
  * Created by SUDREGO on 8/1/2017.
  */
class TransformationFunctions extends LazyLogging {

  def isSignalValid(sName:String) : Boolean = {
    val sigList = List("ISw_Stat","OC_P_ORC","Bckl_Sw_FP_Stat","Bckl_Sw_RL_Stat_SAM_R","Bckl_Sw_RM_Stat_SAM_R","Bckl_Sw_RR_Stat_SAM_R",
    "Odo","GPS_lat","GPS_long","GPS_Alt","Telelog_SW_Version","OCF_Version","ATPL_Version","HU_SW_Version","HU_HW_Version"
      ,"100","101","102","103","104","105","106","107","108","109","110","111","112","200","201","202","203","204","205"
      ,"206","207","208","209","210","211","212","213","214","215","216","217","218","219","300","301","400","401","402"
      ,"403","404","405","500","501","600","601","602","603","604","605","606")

    sigList.contains(sName)
  }

  def MIN_VALID_TIMESTAMP = 1451606400000L
  def MAX_VALID_TIMESTAMP = 1798761600000L

  def getVCDSignalValue(sName:String,sCode : String) : String = {
    val sValue = sName match  {
      case "ISw_Stat" => ISw_Stat(sCode.toInt)
      case "OC_P_ORC" => OC_P_ORC(sCode.toInt)
      case "Bckl_Sw_FP_Stat" => Bckl_Sw_FP_Stat(sCode.toInt)
      case "Bckl_Sw_RL_Stat_SAM_R" => Bckl_Sw_RL_Stat_SAM_R(sCode.toInt)
      case "Bckl_Sw_RM_Stat_SAM_R" => Bckl_Sw_RM_Stat_SAM_R(sCode.toInt)
      case "Bckl_Sw_RR_Stat_SAM_R" => Bckl_Sw_RR_Stat_SAM_R(sCode.toInt)
      case "Odo" => Odo(sCode.toDouble)
      case "GPS_lat" => GPS_lat(sCode.toLong)
      case "GPS_long" => GPS_long(sCode.toLong)
      case "GPS_Alt" => GPS_Alt(sCode.toInt)
      case "Telelog_SW_Version" => Telelog_SW_Version(sCode.toInt)
      case "OCF_Version" => OCF_Version(sCode.toInt)
      case "ATPL_Version" => ATPL_Version(sCode.toInt)
      case "HU_SW_Version" => HU_SW_Version(sCode.toInt)
      case "HU_HW_Version" => HU_HW_Version(sCode.toInt)
    }
    sValue
  }

  // Start of Telelog signal functions

  def ISw_Stat(finput:Int) : String = {
    val ignitionState = finput match {
      case 0 => "IGN_LOCK"
      case 1 => "IGN_OFF"
      case 2 => "IGN_ACC"
      case 3 => "IGN_ON"
      case 4 => "IGN_START"
      case _ => s"${finput} is not a valid ignition state."
    }
    ignitionState
  }

  def OC_P_ORC(finput:Int) : String = {
    val passengerClassification = finput match {
      case 0 => "CLASS0"
      case 1 => "CLASS1"
      case 2 => "CLASS2"
      case 3 => "CLASS3"
      case 4 => "CLASS4"
      case _ => s"Class ${finput} is not a valid passenger classification."
    }
    passengerClassification
  }

  def Bckl_Sw_FP_Stat(finput:Int) : String = {
    val buckleSwitchFrontPassenger = finput match {
      case 0 => "OK"
      case 1 => "NOT"
      case 2 => "FLT"
      case 3 => "NOT_AVAILABLE"
      case _ => s"Input ${finput} is not a valid buckle switch state."
    }
    buckleSwitchFrontPassenger
  }

  def Bckl_Sw_RL_Stat_SAM_R(finput:Int) : String = {
    val buckleSwitchRearLeft = finput match {
      case 0 => "OK"
      case 1 => "NOT"
      case 2 => "FLT"
      case 3 => "NOT_AVAILABLE"
      case _ => s"Input ${finput} is not a valid buckle switch state."
    }
    buckleSwitchRearLeft
  }

  def Bckl_Sw_RM_Stat_SAM_R(finput:Int) : String = {
    val buckleSwitchRearMiddle = finput match {
      case 0 => "OK"
      case 1 => "NOT"
      case 2 => "FLT"
      case 3 => "NOT_AVAILABLE"
      case _ => s"Input ${finput} is not a valid buckle switch state."
    }
    buckleSwitchRearMiddle
  }

  def Bckl_Sw_RR_Stat_SAM_R(finput:Int) : String = {
    val buckleSwitchRearRight = finput match {
      case 0 => "OK"
      case 1 => "NOT"
      case 2 => "FLT"
      case 3 => "NOT_AVAILABLE"
      case _ => s"Input ${finput} is not a valid buckle switch state."
    }
    buckleSwitchRearRight
  }

  def Odo(finput:Double) : String = {
    if (finput >= 0 && finput <= 999999.9) {
      val odometer = finput.toString
      odometer
    }
    else {
      s"Input ${finput} is not within valid range."
    }
  }

  def GPS_lat(finput:Long) : String = {
    val fLat = finput/10000000
    if (fLat >= -90.0000 && fLat <= 90.0000) {
      val positionLat = fLat.toString
      positionLat
    }
    else {
      s"Latitude value ${finput} is invalid."
    }
  }

  def GPS_long(finput:Long) : String = {
    val fLong = finput/10000000
    if (fLong >= -180.0000 && fLong <= 180.0000) {
      val positionLong = fLong.toString
      positionLong
    }
    else {
      s"Longitude value ${finput} is invalid."
    }
  }

  def GPS_Alt(finput:Int) : String = {
    val positionAlt = finput.toString
    positionAlt
  }

  def Telelog_SW_Version(finput:Int) : String = {
    val telelogSwVersion = finput.toString
    telelogSwVersion
  }

  def OCF_Version(finput:Int) : String = {
    val ocfVersion = finput.toString
    ocfVersion
  }

  def HU_SW_Version(finput:Int) : String = {
    val huSwVersion = finput.toString
    huSwVersion
  }

  def HU_HW_Version(finput:Int) : String = {
    val huHwVersion = finput.toString
    huHwVersion
  }

  def ATPL_Version(finput:Int) : String = {
    val atplVersion = finput.toString
    atplVersion
  }

  def getVCDSignalDesc(sName : String) : String = {
    val sigDesc = sName match {
      case "ISw_Stat" => "Ignition switch state"
      case "OC_P_ORC" => "Occupant detection passenger fast"
      case "Bckl_Sw_FP_Stat" => ""
      case "Bckl_Sw_RL_Stat_SAM_R" => "Buckle switch Rear Left"
      case "Bckl_Sw_RM_Stat_SAM_R" => "Buckle switch Rear Middle"
      case "Bckl_Sw_RR_Stat_SAM_R" => "Buckle switch Rear Right"
      case "Odo" => "Odometer"
      case "GPS_lat" => "GPS Latitude"
      case "GPS_long" => "GPS Longitude"
      case "GPS_Alt" => "GPS Altitude"
      case "ATPL_Version" => "Current ATPL ID"
      case "Telelog_SW_Version" => "Telelog system version number"
      case "OCF_Version" => "OCF version number"
      case "HU_SW_Version" => "Head Unit software version number"
      case "HU_HW_Version" => "Head Unit hardware version number"
    }
    sigDesc
  }

  // Start of miscellaneous functions

  def CREATE_DT = new Timestamp((new Date).getTime)

  def getTimestamp(x: Any): java.sql.Timestamp = {
    val format = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss")
    if (x.toString() == "" || x.toString() == "#")
      null
    else {
      val d = format.parse(x.toString())
      val t = new Timestamp(d.getTime())
      t
    }
  }

  def getTimestampFromEpochTime(epochTime : Long) : java.sql.Timestamp = {
    val eventRecvdTimestamp = new DateTime(epochTime).toDateTime.toString("yyyy/MM/dd hh:mm:ss")
    getTimestamp(eventRecvdTimestamp)
  }

  def getEpochTimeFromTimestamp(currTime : String) : Long = {
    //val str = (new Timestamp((new Date).getTime)).toString
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val date = df.parse(currTime)
    val epochTS = date.getTime
    epochTS
  }

  def writeToParquet(df : DataFrame,path : String) = {
    //logger.info("Writing the dataframe : " +df.show(50) + " to parquet file")
    df.write.mode(SaveMode.Append).parquet(path)
  }

  def rtrim(s: String) = s.replaceAll("\\s+$", "")

  def stfSignalNameGroupMapping(sName : String) : String = {
    val sigGrp = sName match {
      case "getActiveApplication" => "ApplicationControlService"
      case "setActiveApplication" => "ApplicationControlService"
      case "activeApplicationChanged" => "ApplicationControlService"
      case "getCurrentSystemLanguage" => "ApplicationControlService"
      case "systemLanguageChanged" => "ApplicationControlService"
      case "getASAContentType" => "ApplicationControlService"
      case "setASAContentType" => "ApplicationControlService"
      case "getASAStatus" => "ApplicationControlService"
      case "setASAStatus" => "ApplicationControlService"
      case "asaStatusChanged" => "ApplicationControlService"
      case "asaContentTypeChanged" => "ApplicationControlService"
      case "getICDDisplayStatus" => "ICDDataManagerService"
      case "icdDisplayStatusNotification" => "ICDDataManagerService"
      case "cceKeyStatusChanged" => "CCEInputService"
      case "currentIndexChanged" => "CCEInputService"
      case "relativeIndexChanged" => "CCEInputService"
      case "hardKeyStatusChanged" => "HardKeyInputService"
      case "ofnEventChanged" => "OFNInputService"
      case "positionChanged(OFN)" => "OFNInputService"
      case "remoteControlCommandChanged" => "RemoteControlInputService"
      case "remoteControlledDisplayChanged" => "RemoteControlInputService"
      case "positionChanged(Touchpad)" => "TouchpadInputService"
      case "touchStateChanged" => "TouchpadInputService"
      case "sdsStatusChanged" => "SDSStatusService"
      case "gasBrandPOICategoryListChanged" => "AddressSearch"
      case "drowsinessPOICategoryListChanged" => "AddressSearch"
      case "quickPOICategoryListChanged" => "AddressSearch"
      case "getDrowsinessPOICategoryList" => "AddressSearch"
      case "getQuickPOICategoryList" => "AddressSearch"
      case "getGasBrandPOICategoryList" => "AddressSearch"
      case "destinationReached" => "Guiding"
      case "guideStatusChanged" => "Guiding"
      case "betterGuideRouteChanged" => "Guiding"
      case "guideLevelChanged" => "Guiding"
      case "getGuideStatus" => "Guiding"
      case "getGuideLevel" => "Guiding"
      case "mapSkinTypeChanged" => "MapDisplayContent"
      case "poiVisibilityChanged" => "MapDisplayContent"
      case "trafficVisibilityChanged" => "MapDisplayContent"
      case "fuelRangeVisibilityChanged" => "MapDisplayContent"
      case "satelliteMapVisibilityChanged" => "MapDisplayContent"
      case "trafficMapStyleChanged" => "MapDisplayContent"
      case "trafficCamVisibilityChanged" => "MapDisplayContent"
      case "weatherVisibilityChanged" => "MapDisplayContent"
      case "getMapSkinType" => "MapDisplayContent"
      case "getPOIVisibility" => "MapDisplayContent"
      case "isWeatherVisibility" => "MapDisplayContent"
      case "getTrafficVisibility" => "MapDisplayContent"
      case "isFuelRangeVisibility" => "MapDisplayContent"
      case "isSatelliteMapVisibility" => "MapDisplayContent"
      case "isTrafficMapStyle" => "MapDisplayContent"
      case "isTrafficCamVisibility" => "MapDisplayContent"
      case "mapModeChanged" => "MapDisplayControl"
      case "mapAutoOrientationChanged" => "MapDisplayControl"
      case "mapViewChanged" => "MapDisplayControl"
      case "mapScaleChanged" => "MapDisplayControl"
      case "getMapMode" => "MapDisplayControl"
      case "getMapAutoOrientation" => "MapDisplayControl"
      case "getMapView" => "MapDisplayControl"
      case "isMapAutozoom" => "MapDisplayControl"
      case "attentionAssistStatusChanged" => "NaviStatus"
      case "trailerAttachedStatusChanged" => "NaviStatus"
      case "foregroundChanged" => "NaviStatus"
      case "getStorageNumberOfEntries" => "ObjectStorage"
      case "storageStatusChanged" => "ObjectStorage"
      case "getStorageStatus" => "ObjectStorage"
      case "getStorageAvailablePercentage" => "Routing"
      case "routeModeAndOptionsChanged" => "Routing"
      case "getRouteModeAndOptions" => "Routing"
      case "callStatusChanged" => "CallManagerService"
      case "considerAllTripsEnabledChanged" => "DestinationPredictionService"
      case "destinationPredictionEnabledChanged" => "DestinationPredictionService"
      case "resetPredictionModelStatusChanged" => "DestinationPredictionService"
    }
    sigGrp
  }

  def SESignalMapping(finput:Int) : Array[String] = {
    var SignalInformation = new Array[String](2)
    SignalInformation = finput match {
      case 100 => Array("rebootGeneral","HU reboot general")
      case 101 => Array("rebootWdtReset","HU reboot V-CPU reset (WDT)")
      case 102 => Array("rebootPlusBOff","HU reboot +B off")
      case 103 => Array("rebootUltraLowVoltage","HU reboot ultra-low voltage detection")
      case 104 => Array("rebootMainSanity","HU reboot main sanity")
      case 105 => Array("rebootManualReset","HU reboot manual reset")
      case 106 => Array("rebootEcuHardReset","HU reboot ECU hard reset")
      case 107 => Array("rebootEcuSoftReset","HU reboot ECU soft reset")
      case 108 => Array("rebootSwdlSuccess","HU reboot SWDL success")
      case 109 => Array("rebootEmReset","HU reboot engineering mode reset")
      case 110 => Array("rebootUserDataImport","HU reboot user data import")
      case 111 => Array("rebootUserDataReset","HU reboot user data reset")
      case 112 => Array("rebootWdtSwReset","HU reboot V-CPU (S/W WDT)")
      case 200 =>  Array("shutdownGeneral","HU shutdown general")
      case 201 =>  Array("shutdownHuCanAbnormal","HU shutdown HU CAN abnormal")
      case 202 => Array("shutdownHmiCanAbnormal","HU shutdown HMI CAN abnormal")
      case 203 => Array("shutdownHighTemperature","HU shutdown high temperature")
      case 204 => Array("shutdownHighAppVoltage","HU shutdown high voltage (Voltage Application Critical)")
      case 205 => Array("shutdownLowAppVoltage","HU shutdown low voltage (Voltage Application Critical")
      case 206 => Array("shutdownLowVoltage","HU shutdown low voltage (Voltage Critical)")
      case 207 => Array("shutdownLimpHome","HU shutdown limp home")
      case 208 => Array("shutdownBatteryCutOff","HU shutdown battery cut off")
      case 209 => Array("shutdownPressOnKey","HU shutdown press on key")
      case 210 => Array("shutdownByIgnition","HU shutdown ignition switch")
      case 211 => Array("shutdownTransportMode","HU shutdown transport mode")
      case 212 => Array("shutdownProductionMode","HU shutdown production mode")
      case 213 => Array("shutdownLowTemperature","HU shutdown low temperature")
      case 214 => Array("shutdownIhtmModeEnd","HU shutdown IHTM mode end")
      case 215 => Array("shutdownDiagEnd","HU shutdown Diag end")
      case 216 => Array("shutdownMostAppVoltageAbnormal","HU shutdown MOST voltage abnormal (Application Critical)")
      case 217 => Array("shutdownMostVoltageAbnormal","HU shutdown MOST voltage abnormal")
      case 218 => Array("shutdownUndefinedScenario","HU shutdown undefined shutdown scenario")
      case 219 => Array("shutdownUndefined","HU shutdown undefined")
      case 300 => Array("startUserOffMode","HU displays enters into the user-off mode")
      case 301 => Array("stopUserOffMode","HU displays come back from user-off")
      case 400 => Array("telelogEnabled","Telelog enabled by backend")
      case 401 => Array("telelogDisabled","Telelog disabled by backend")
      case 402 => Array("telelogEnabledByEM","Telelog is enabled by Engineering Menu")
      case 403 => Array("telelogDisabledByEM","Telelog is disabled by Engineering Menu")
      case 404 => Array("telelogNewATPL","Telelog ATPL is changed by DaiVB")
      case 500 => Array("telelogConfigChange","Telelog configuration change by backend")
      case 501 => Array("telelogConfigChangeByEM","Telelog configuration change by Engineering Menu")
      case 600 => Array("telelogSftThresholdOver","Telelog SFT threshold is over")
      case 601 => Array("telelogVcdThresholdOver","Telelog VCD threshold is over")
      case 602 => Array("telelogGzipError","TFTelog-704")
      case 603 => Array("telelogStartupIF1GetError","FTelog-705 case 1")
      case 604 => Array("telelgStartupIF1GetNoResponse","Ftelog-705 case 2")
      case 605 => Array("telelogHMIManagerError","FTelog-706")
      case 606 => Array("telelogSubscribeIF1Error","Subscribe is Error")
      case _ => Array("ERROR","NO SUCH TRANSFORMATION EVENT ID")
    }
    SignalInformation
  }

  def parseSEEventData(ed:String) : String = {
    logger.info("The input argument event data is : " +ed)
    if(ed.length != 0) {
      logger.info("Input argument is not null")
      val eventDataTmpLen = ed.length
      //val eventData = ed.substring(0,eventDataTmpLen - 1)
      val eventData = ed.replaceAll("\"","")
      eventData
    } else {
      val eventData = ""
      eventData
    }
  }

  // START 0F STF FUNCTIONS

  // ****************<<ICManager>> ****************************************** //

  def getICDDisplayStatus(finput: String, foutPut: String): MutableMap[String, String] = {

    val displayAreaMap = Map("1" -> "MAIN_MENU_AREA", "2" -> "SELECTABLE_CONTENTS_AREA", "4" -> "HEAD_UP_DISPLAY")
    val applicationTypeMap = Map("0" -> "NEUTRAL", "1" -> "RADIO", "2" -> "MEDIA", "3" -> "TELEPHONE"
      , "4" -> "NAVIGATION", "6" -> "BROWSER", "255" -> "UNKNOWN")

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val displayArea = signalInputVal(0).trim
    val clientId = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (displayAreaMap.contains(displayArea) && applicationTypeMap.contains(foutPut.trim)) {

      signalMappingVal.put("displayArea", displayAreaMap(displayArea))
      signalMappingVal.put("ClientId", clientId)
      signalMappingVal.put("applicationType", applicationTypeMap(foutPut.trim))

    } else {
      logger.info("The value ${finOutPut} for parameter applicationType or ${displayArea} for parameter displayArea is not valid.")
    }
    signalMappingVal
  }

  def icdDisplayStatusNotification(finput: String, foutPut: String): MutableMap[String, String] = {

    val displayAreaMap = Map("1" -> "MAIN_MENU_AREA", "2" -> "SELECTABLE_CONTENTS_AREA", "4" -> "HEAD_UP_DISPLAY")
    val applicationTypeMap = Map("0" -> "NEUTRAL", "1" -> "RADIO", "2" -> "MEDIA", "3" -> "TELEPHONE"
      , "4" -> "NAVIGATION", "6" -> "BROWSER", "255" -> "UNKNOWN")

    var signalInputVal = new Array[String](3)
    signalInputVal = finput.split(",")
    val displayArea = signalInputVal(0)
    val applicationType = signalInputVal(1)
    val clientId = signalInputVal(2)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (displayAreaMap.contains(displayArea) && applicationTypeMap.contains(applicationType)) {

      signalMappingVal.put("displayArea", displayAreaMap(displayArea))
      signalMappingVal.put("ClientId", clientId)
      signalMappingVal.put("applicationType", applicationTypeMap(applicationType))

    } else {
      logger.info("The value ${applicationType} for parameter applicationType or ${displayArea} for parameter displayArea is not valid.")
    }
    signalMappingVal
  }

  // ****************<<InputManager>>******************************************//

  def cceKeyStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val cceKeyCodeMap = Map("0" -> "UNKNOWN", "1" -> "NORTH", "2" -> "NORTH_EAST","3" -> "EAST"
      , "4" -> "SOUTH_EAST", "5" -> "SOUTH", "6" -> "SOUTH_WEST", "7" -> "WEST", "8" -> "NORTH_WEST", "9" -> "CENTRAL")
    val cceKeyStateMap = Map("0"-> "UNKNOWN", "1" -> "UP", "2" -> "DOWN")

    var signalInputVal = new Array[String](3)
    signalInputVal = finput.split(",")
    val keyCode = signalInputVal(0)
    val keyState = signalInputVal(1)
    val sequenceID = signalInputVal(2)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (cceKeyCodeMap.contains(keyCode) && cceKeyStateMap.contains(keyState)) {

      signalMappingVal.put("keyCode", cceKeyCodeMap(keyCode))
      signalMappingVal.put("keyState", cceKeyStateMap(keyState))
      signalMappingVal.put("sequenceID", sequenceID)

    } else {
      logger.info("The value ${keyCode} for parameter keyCode or ${keyState} for parameter keyState is not valid.")
    }
    signalMappingVal
  }

  def currentIndexChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    var signalInputVal = new Array[String](4)
    signalInputVal = finput.split(",")
    val currentIndex = signalInputVal(0)
    val currentMaxIndex = signalInputVal(1)
    val sequenceID = signalInputVal(2)
    val clientID = signalInputVal(3)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    signalMappingVal.put("currentIndex", currentIndex)
    signalMappingVal.put("currentMaxIndex", currentMaxIndex)
    signalMappingVal.put("sequenceID", sequenceID)
    signalMappingVal.put("clientID", clientID)

    signalMappingVal
  }

  def relativeIndexChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    var signalInputVal = new Array[String](3)
    signalInputVal = finput.split(",")
    val rotation = signalInputVal(0)
    val sequenceID = signalInputVal(1)
    val clientID = signalInputVal(2)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    signalMappingVal.put("rotation", rotation)
    signalMappingVal.put("sequenceID", sequenceID)
    signalMappingVal.put("clientID", clientID)

    signalMappingVal
  }

  def hardKeyStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val keyCodeMap = Map("0" -> "UNKNOWN", "1" -> "ON", "2" -> "BACK", "3" -> "MENU", "4" -> "MUTE", "5" -> "NAVI", "6" -> "MEDIA", "7" -> "RADIO",
      "8" -> "PHONE", "9" -> "VIDEO", "10" -> "CAR", "11" -> "SEAT", "15" -> "BGA", "32" -> "NUMPAD_0", "33" -> "NUMPAD_1", "34" -> "NUMPAD_2",
      "35" -> "NUMPAD_3", "36" -> "NUMPAD_4", "37" -> "NUMPAD_5", "38" -> "NUMPAD_6", "39" -> "NUMPAD_7", "40" -> "NUMPAD_8", "41" -> "NUMPAD_9",
      "42" -> "NUMPAD_STAR", "43" -> "NUMPAD_POUND", "44" -> "NUMPAD_CLEAR", "64" -> "SEND", "65" -> "END", "67" -> "PTT", "68" -> "FAVORITE",
      "69" -> "VOLUME_UP", "70" -> "VOLUME_DOWN", "71" -> "VOLUME_WHEEL_UP", "72" -> "VOLUME_WHEEL_DOWN", "80" -> "EJECT", "81" -> "SKIP_BW",
      "82" -> "SKIP_FW", "112" -> "WEB", "113" -> "PLAY_PAUSE")
    val keyStateMap = Map("0" -> "UNKNOWN", "1" -> "UP", "2" -> "DOWN")
    val affectedDisplayMap = Map("0" -> "DRIVER", "1" -> "PASSENGER", "2" -> "REAR_LEFT", "3" -> "REAR_RIGHT")

    var signalInputVal = new Array[String](2)

    val keyCodePattern = """keyCode:(\d+)""".r
    val keyStatePattern = """keyState:(\d+)""".r
    val affectedDisplayPattern = """affectedDisplay:(\d+)""".r

    signalInputVal = keyCodePattern.findFirstIn(finput).toString().split(":")
    val keyCode = signalInputVal(1).dropRight(1)
    signalInputVal = keyStatePattern.findFirstIn(finput).toString().split(":")
    val keyState = signalInputVal(1).dropRight(1)
    signalInputVal = affectedDisplayPattern.findFirstIn(finput).toString().split(":")
    val affectedDisplay = signalInputVal(1).dropRight(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (keyCodeMap.contains(keyCode) && keyStateMap.contains(keyState) && affectedDisplayMap.contains(affectedDisplay)) {

      signalMappingVal.put("keyCode", keyCodeMap(keyCode))
      signalMappingVal.put("keyState", keyStateMap(keyState))
      signalMappingVal.put("affectedDisplay", affectedDisplayMap(affectedDisplay))

    } else {
      logger.info("The value ${keyCode} for parameter keyCode or ${keyState} for parameter keyState is not valid or ${affectedDisplay} for parameter affectedDisplay is not valid")
    }
    signalMappingVal
  }

  def positionChanged_OFN(finput: String, foutPut: String): MutableMap[String, String] = {

    var signalOutPutVal = new Array[String](2)
    val xPattern = """x:(\d+)""".r
    val yPattern = """y:(\d+)""".r
    var inputVal = new Array[String](2)

    signalOutPutVal = xPattern.findFirstIn(finput).toString().split(":")
    val x = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = yPattern.findFirstIn(finput).toString().split(":")
    val y = signalOutPutVal(1).dropRight(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    signalMappingVal.put("Position X", x)
    signalMappingVal.put("Position Y", y)
    signalMappingVal
  }

  def positionChanged_Touchpad(finput: String, foutPut: String): MutableMap[String, String] = {

    val positionTypeMap = Map("0" -> "RECOGNITION_POS", "1" -> "GESTURE_POS")
    var signalOutPutVal = new Array[String](2)
    val xPattern = """x:(\d+)""".r
    val yPattern = """y:(\d+)""".r
    val positionTypePattern = """,(\d+)""".r
    var inputVal = new Array[String](2)

    signalOutPutVal = xPattern.findFirstIn(finput).toString().split(":")
    val x = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = yPattern.findFirstIn(finput).toString().split(":")
    val y = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = positionTypePattern.findFirstIn(finput).toString().split(",")
    val positionType = signalOutPutVal(1).dropRight(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (positionTypeMap.contains(positionType)) {

      signalMappingVal.put("Position X", x)
      signalMappingVal.put("Position Y", y)
      signalMappingVal.put("positionType", positionTypeMap(positionType))

    } else {
      logger.info("The value ${positionType} for parameter positionType is not valid.")
    }
    signalMappingVal
  }

  def touchStateChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val currentTouchStateMap = Map("0" -> "NO_TOUCH", "1" -> "TOUCH", "255" -> "INVALID")
    var signalOutPutVal = new Array[String](2)
    val currentTouchStatePattern = """currentTouchState:(\d+)""".r
    val numberOfFingersPattern = """numberOfFingers:(\d+)""".r
    var inputVal = new Array[String](2)

    signalOutPutVal = currentTouchStatePattern.findFirstIn(finput).toString().split(":")
    val currentTouchState = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = numberOfFingersPattern.findFirstIn(finput).toString().split(":")
    val numberOfFingers = signalOutPutVal(1).dropRight(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (currentTouchStateMap.contains(currentTouchState)) {

      signalMappingVal.put("touchState : currentTouchState", currentTouchStateMap(currentTouchState))
      signalMappingVal.put("touchState :  numberOfFingers", numberOfFingers)

    } else {
      logger.info("The value ${currentTouchState} for parameter currentTouchState is not valid.")
    }
    signalMappingVal
  }

  def ofnEventChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val eventCodeMap = Map("0" -> "DRAG_NORTH", "1" -> "DRAG_EAST", "2" -> "DRAG_SOUTH", "4" -> "DRAG_WEST", "5" -> "PRESS", "6" -> "RELEASE", "255" -> "UNKNOWN")
    val hmiModeMap = Map("0" -> "MODE_01", "1" -> "MODE_02", "2" -> "MODE_03", "3" -> "MODE_04", "4" -> "MODE_05", "5" -> "MODE_06", "6" -> "MODE_07",
      "7" -> "MODE_08", "8" -> "MODE_09", "9" -> "MODE_10", "10" -> "MODE_11", "11" -> "MODE_12", "12" -> "MODE_13")

    var signalOutPutVal = new Array[String](5)

    val mapPattern = """\{widgetID:(\d+),hmiMode:(\d+)\},(\d+)""".r
    val inputPattern = """(\d+),(\d+),(\d+)""".r
    val clientPattern = """,(\d+)""".r
    val widgetIDPattern = """widgetID:(\d+)""".r
    val hmiModePattern = """hmiMode:(\d+)""".r

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    var inputVal = new Array[String](2)
    inputVal(0) = inputPattern.findFirstIn(finput).toString()
    inputVal(1) = mapPattern.findFirstIn(finput).toString()

    signalOutPutVal = widgetIDPattern.findFirstIn(inputVal(1)).toString().split(":")
    val widgetID = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = hmiModePattern.findFirstIn(inputVal(1)).toString().split(":")
    val hmiMode = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = clientPattern.findFirstIn(inputVal(1)).toString().split(",")
    val clientId = signalOutPutVal(1).dropRight(1)

    var parseInputVal = new Array[String](3)
    parseInputVal = inputVal(0).split(",")
    val eventCode = parseInputVal(0).drop(5)
    val numberOfEvents = parseInputVal(1)
    val isFirstEvent = Integer.parseInt(parseInputVal(2).dropRight(1))

    if ((eventCodeMap.contains(eventCode) && hmiModeMap.contains(hmiMode)) && (isFirstEvent == 0 || isFirstEvent == 1)) {

      signalMappingVal.put("eventCode", eventCodeMap(eventCode))
      signalMappingVal.put("numberOfEvents", numberOfEvents)
      signalMappingVal.put("isFirstEvent", isFirstEvent.toString())
      signalMappingVal.put("widgetID", widgetID)
      signalMappingVal.put("hmiMode", hmiModeMap(hmiMode))
      signalMappingVal.put("clientId", clientId)

    } else {
      printf("The value ${eventCode} for parameter eventCode is not valid or The value ${isFirstEvent} for parameter isFirstEvent is not valid or The value ${hmiMode} for parameter hmiMode is not valid.")
    }
    signalMappingVal
  }

  //*************************<<Main>>*************************************

  def getActiveApplication(finput: String, foutPut: String): MutableMap[String, String] = {

    val applicationMap = Map("0" -> "NAVI", "1" -> "RADIO", "2" -> "PLAYER", "3" -> "TEL"
      , "4" -> "VEHICLE", "5" -> "SYSTEM_SETTINGS", "6" -> "SEAT", "7" -> "CLIMATE", "8" -> "TV", "11" -> "BROWSER", "10" -> "HOME",
      "12" -> "PARK_MAN", "13" -> "ENGINEERING_MODE", "14" -> "DEALER_MODE", "15" -> "ECM", "16" -> "MIRROR_LINK", "17" -> "SPV",
      "18" -> "ACTIVE_COMFORT", "19" -> "MESSAGING", "20" -> "E_CALL", "21" -> "MB_APPS", "22" -> "DIBA", "23" -> "ENERGY", "24" -> "TIME_AND_DATE",
      "25" -> "USER_DATA", "26" -> "ADDRESS_BOOK", "27" -> "CAR_PLAY", "28" -> "ANDROID_AUTO", "29" -> "CONNECT_MENU", "30" -> "DYNAMIC_SELECT",
      "32" -> "AMG_APP")

    val application = foutPut
    val clientId = finput
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (applicationMap.contains(application)) {
      signalMappingVal.put("clientId", clientId)
      signalMappingVal.put("application", applicationMap(application))

    } else {
      logger.info("The value ${application} for parameter Application is not valid.")
    }
    signalMappingVal
  }

  def setActiveApplication(finput: String, foutPut: String): MutableMap[String, String] = {

    val applicationMap = Map("0" -> "NAVI", "1" -> "RADIO", "2" -> "PLAYER", "3" -> "TEL"
      , "4" -> "VEHICLE", "5" -> "SYSTEM_SETTINGS", "6" -> "SEAT", "7" -> "CLIMATE", "8" -> "TV", "11" -> "BROWSER"
      , "10" -> "HOME", "12" -> "PARK_MAN", "13" -> "ENGINEERING_MODE", "14" -> "DEALER_MODE", "15" -> "ECM"
      , "16" -> "MIRROR_LINK", "17" -> "SPV", "18" -> "ACTIVE_COMFORT", "19" -> "MESSAGING", "20" -> "E_CALL"
      , "21" -> "MB_APPS", "22" -> "DIBA", "23" -> "ENERGY", "24" -> "TIME_AND_DATE", "25" -> "USER_DATA", "26" -> "ADDRESS_BOOK"
      , "27" -> "CAR_PLAY", "28" -> "ANDROID_AUTO", "29" -> "CONNECT_MENU", "30" -> "DYNAMIC_SELECT", "32" -> "AMG_APP")

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val activeApplication = signalInputVal(0)
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (applicationMap.contains(activeApplication)) {
      signalMappingVal.put("activeApplication", applicationMap(activeApplication))
      signalMappingVal.put("clientID",clientID )
    } else {
      logger.info("The value ${activeApplication} for parameter activeApplication is not valid.")
    }
    signalMappingVal
  }

  def activeApplicationChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val applicationMap = Map("0" -> "NAVI", "1" -> "RADIO", "2" -> "PLAYER", "3" -> "TEL"
      , "4" -> "VEHICLE", "5" -> "SYSTEM_SETTINGS", "6" -> "SEAT", "7" -> "CLIMATE", "8" -> "TV", "11" -> "BROWSER",
      "10" -> "HOME", "12" -> "PARK_MAN", "13" -> "ENGINEERING_MODE", "14" -> "DEALER_MODE", "15" -> "ECM", "16" -> "MIRROR_LINK", "17" -> "SPV",
      "18" -> "ACTIVE_COMFORT", "19" -> "MESSAGING", "20" -> "E_CALL", "21" -> "MB_APPS", "22" -> "DIBA", "23" -> "ENERGY",
      "24" -> "TIME_AND_DATE", "25" -> "USER_DATA", "26" -> "ADDRESS_BOOK", "27" -> "CAR_PLAY", "28" -> "ANDROID_AUTO", "29" -> "CONNECT_MENU",
      "30" -> "DYNAMIC_SELECT", "32" -> "AMG_APP")

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val activeApplication = signalInputVal(0)
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (applicationMap.contains(activeApplication)) {
      signalMappingVal.put("activeApplication", applicationMap(activeApplication))
      signalMappingVal.put("clientID",clientID )
    } else {
      logger.info("The value ${activeApplication} for parameter activeApplication is not valid.")
    }
    signalMappingVal
  }

  def getASAContentType(finput: String, foutPut: String): MutableMap[String, String] = {

    val contentTypeMap = Map("0" -> "DEFAULT", "1" -> "NAVI_MAP", "2"-> "ENTERTAINMENT"
      , "3" -> "ENERGY_FLOW", "4" -> "CONSUMPTION", "5" -> "DATE_TIME", "6" -> "WEATHER")

    val clientID = finput
    val contentType = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (contentTypeMap.contains(contentType)) {
      signalMappingVal.put("clientID",clientID )
      signalMappingVal.put("contentType", contentTypeMap(contentType))
    } else {
      logger.info("The value ${contentType} for parameter contentType is not valid.")
    }
    signalMappingVal
  }

  def setASAContentType(finput: String, foutPut: String): MutableMap[String, String] = {

    val contentTypeMap = Map("0" -> "DEFAULT", "1" -> "NAVI_MAP", "2"-> "ENTERTAINMENT"
      , "3" -> "ENERGY_FLOW", "4" -> "CONSUMPTION", "5" -> "DATE_TIME", "6" -> "WEATHER")

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val contentType = signalInputVal(0)
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (contentTypeMap.contains(contentType)) {
      signalMappingVal.put("contentType", contentTypeMap(contentType))
      signalMappingVal.put("clientID",clientID )
    } else {
      logger.info("The value ${contentType} for parameter contentType is not valid.")
    }
    signalMappingVal
  }

  def getASAStatus(finput: String, foutPut: String): MutableMap[String, String] = {

    val clientID = finput
    val isActivated = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isActivated == 0 || isActivated == 1) {
      signalMappingVal.put("clientID",clientID )
      signalMappingVal.put("isActivated",isActivated.toString)
    } else {
      logger.info("The value ${isActivated} for parameter isActivated is not valid.")
    }
    signalMappingVal
  }

  def setASAStatus(finput: String, foutPut: String): MutableMap[String, String] = {

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val isActivated = Integer.parseInt(signalInputVal(0))
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isActivated == 0 || isActivated == 1) {
      signalMappingVal.put("clientID",clientID )
      signalMappingVal.put("isActivated",isActivated.toString)
    } else {
      logger.info("The value ${isActivated} for parameter isActivated is not valid.")
    }
    signalMappingVal
  }

  def asaStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val isActivated = Integer.parseInt(signalInputVal(0))
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isActivated == 0 || isActivated == 1) {
      signalMappingVal.put("clientID",clientID )
      signalMappingVal.put("isActivated",isActivated.toString)
    } else {
      logger.info("The value ${isActivated} for parameter isActivated is not valid.")
    }
    signalMappingVal
  }

  def asaContentTypeChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val contentTypeMap = Map("0" -> "DEFAULT", "1" -> "NAVI_MAP", "2"-> "ENTERTAINMENT"
      , "3" -> "ENERGY_FLOW", "4" -> "CONSUMPTION", "5" -> "DATE_TIME", "6" -> "WEATHER")
    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val contentType = signalInputVal(0)
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (contentTypeMap.contains(contentType)) {
      signalMappingVal.put("contentType",contentTypeMap(contentType))
      signalMappingVal.put("clientID",clientID )
    } else {
      logger.info("The value ${contentType} for parameter contentType is not valid.")
    }
    signalMappingVal
  }

  def getCurrentSystemLanguage(finput: String, foutPut: String): MutableMap[String, String] = {

    val seatPositionMap = Map("0" -> "FRONT_LEFT", "1" -> "FRONT_RIGHT", "2" -> "REAR_LEFT")
    var signalInputVal = new Array[String](2)
    var signalOutPutVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val seatPosition = signalInputVal(0)
    val clientID = signalInputVal(1)

    val idPattern = """id:(\d+)""".r
    val languageISOCodePattern = """languageISOCode:(\w+)""".r
    val countryISOCodePattern = """countryISOCode:(\w+)""".r
    val scriptISOCodePattern = """scriptISOCode:(\w+)""".r

    signalOutPutVal = idPattern.findFirstIn(foutPut).toString().split(":")
    val id = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = languageISOCodePattern.findFirstIn(foutPut).toString().split(":")
    val languageISOCode = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = countryISOCodePattern.findFirstIn(foutPut).toString().split(":")
    val countryISOCode = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = scriptISOCodePattern.findFirstIn(foutPut).toString().split(":")
    val scriptISOCode = signalOutPutVal(1).dropRight(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (seatPositionMap.contains(seatPosition)) {
      signalMappingVal.put("seatPosition", seatPositionMap(seatPosition))
      signalMappingVal.put("clientID", clientID)
      signalMappingVal.put("id", id)
      signalMappingVal.put("languageISOCode", languageISOCode)
      signalMappingVal.put("countryISOCode", countryISOCode)
      signalMappingVal.put("scriptISOCode", scriptISOCode)

    } else {
      logger.info("The value ${seatPosition} for parameter seatPosition is not valid.")
    }
    signalMappingVal
  }

  def systemLanguageChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val seatPositionMap = Map("0" -> "FRONT_LEFT", "1" -> "FRONT_RIGHT", "2" -> "REAR_LEFT")
    def statusMap = Map("0" -> "LOADED", "1" -> "LOADING", "2" -> "FAILURE")
    var signalOutPutVal = new Array[String](2)

    val languagePattern = """\{id:(\d+),languageISOCode:(\w+),countryISOCode:(\w+),scriptISOCode:(\w+)}""".r
    val inputPattern = """(\d+),(\d+),(\d+)""".r
    val idPattern = """id:(\d+)""".r
    val languageISOCodePattern = """languageISOCode:(\w+)""".r
    val countryISOCodePattern = """countryISOCode:(\w+)""".r
    val scriptISOCodePattern = """scriptISOCode:(\w+)""".r

    var inputVal = new Array[String](2)
    inputVal(0) = languagePattern.findFirstIn(finput).toString()
    inputVal(1) = inputPattern.findFirstIn(finput).toString()

    signalOutPutVal = idPattern.findFirstIn(inputVal(0)).toString().split(":")
    val id = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = languageISOCodePattern.findFirstIn(inputVal(0)).toString().split(":")
    val languageISOCode = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = countryISOCodePattern.findFirstIn(inputVal(0)).toString().split(":")
    val countryISOCode = signalOutPutVal(1).dropRight(1)
    signalOutPutVal = scriptISOCodePattern.findFirstIn(inputVal(0)).toString().split(":")
    val scriptISOCode = signalOutPutVal(1).dropRight(1)

    var parseInputVal = new Array[String](3)
    parseInputVal = inputVal(1).split(",")
    val status = parseInputVal(0).drop(5)
    val seatPosition = parseInputVal(1)
    val clientID= parseInputVal(2).dropRight(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (seatPositionMap.contains(seatPosition) && statusMap.contains(status)) {
      signalMappingVal.put("id", id)
      signalMappingVal.put("languageISOCode", languageISOCode)
      signalMappingVal.put("countryISOCode", countryISOCode)
      signalMappingVal.put("scriptISOCode", scriptISOCode)
      signalMappingVal.put("status", statusMap(status))
      signalMappingVal.put("seatPosition", seatPositionMap(seatPosition))
      signalMappingVal.put("clientID", clientID)

    } else {
      logger.info("The value ${seatPosition} for parameter seatPosition is not valid or The value ${status} for parameter status is not valid.")
    }
    signalMappingVal
  }

  //****************<<MapDisplayControl>>******************************************//

  def mapModeChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val mapModeMap = Map("2" -> "POSITION", "1" ->  "SCROLLING", "32" -> "ROTATION", "3" -> "DETOUR", "4" -> "IC_CORRECT",
      "5" -> "ROUTE_MONITOR", "6" -> "ROUTE_FLIGHT", "7" -> "OBJECT3D", "8" -> "ASA_POSITION",
      "10" -> "POSITION_AND_OBJECT", "11" -> "CENTER_AND_OBJECT", "12" -> "OBJECT",
      "13" -> "ROUTE", "14" -> "EN_ROUTE_SCROLL", "15" -> "SPOTLIGHT", "34" -> "TRAFFIC_ROUTE")

    val MapMode = finput
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (mapModeMap.contains(MapMode)) {
      signalMappingVal.put("MapMode",mapModeMap(MapMode))
    } else {
      logger.info("The value ${MapMode} for parameter MapMode is not valid.")
    }
    signalMappingVal
  }

  def mapAutoOrientationChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val MapAutoOrientationMap = Map("1" -> "NORTHUP", "2" -> "HEADUP")

    val MapAutoOrientation = finput
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (MapAutoOrientationMap.contains(MapAutoOrientation)) {
      signalMappingVal.put("MapAutoOrientation",MapAutoOrientationMap(MapAutoOrientation))
    } else {
      logger.info("The value ${MapAutoOrientation} for parameter MapAutoOrientation is not valid.")
    }
    signalMappingVal
  }

  def mapViewChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val mapViewMap = Map("1" -> "VIEW2D", "2" -> "VIEW3D")
    val MapView = finput
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (mapViewMap.contains(MapView)) {
      signalMappingVal.put("MapView",mapViewMap(MapView))
    } else {
      logger.info("The value ${MapView} for parameter MapView is not valid.")
    }
    signalMappingVal
  }

  def mapScaleChanged(finput: String, foutPut: String): MutableMap[String, String] = {
    val inputVal = Integer.parseInt(finput)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (inputVal >= 1 && inputVal <= 12) {
      val MapScaleStep = "STEP_" + inputVal.toString
      signalMappingVal.put("MapScaleStep",MapScaleStep)
    } else {
      logger.info("The value ${MapScaleStep} for parameter MapScaleStep is not valid.")
    }
    signalMappingVal
  }

  def getMapMode(finput: String, foutPut: String): MutableMap[String, String] = {

    val mapModeMap = Map("2" -> "POSITION", "1" ->  "SCROLLING", "32" -> "ROTATION", "3" -> "DETOUR", "4" -> "IC_CORRECT",
      "5" -> "ROUTE_MONITOR", "6" -> "ROUTE_FLIGHT", "7" -> "OBJECT3D", "8" -> "ASA_POSITION",
      "10" -> "POSITION_AND_OBJECT", "11" -> "CENTER_AND_OBJECT", "12" -> "OBJECT",
      "13" -> "ROUTE", "14" -> "EN_ROUTE_SCROLL", "15" -> "SPOTLIGHT", "34" -> "TRAFFIC_ROUTE")

    val MapMode = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (mapModeMap.contains(MapMode)) {
      signalMappingVal.put("MapMode",mapModeMap(MapMode))
    } else {
      logger.info("The value ${MapMode} for parameter MapMode is not valid.")
    }
    signalMappingVal
  }

  def getMapAutoOrientation(finput: String, foutPut: String): MutableMap[String, String] = {

    val MapAutoOrientationMap = Map("1" -> "NORTHUP", "2" -> "HEADUP")

    val MapAutoOrientation = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (MapAutoOrientationMap.contains(MapAutoOrientation)) {
      signalMappingVal.put("MapAutoOrientation",MapAutoOrientationMap(MapAutoOrientation))
    } else {
      logger.info("The value ${MapAutoOrientation} for parameter MapAutoOrientation is not valid.")
    }
    signalMappingVal
  }

  def getMapView(finput: String, foutPut: String): MutableMap[String, String] = {

    val mapViewMap = Map("1" -> "VIEW2D", "2" -> "VIEW3D")
    val MapView = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (mapViewMap.contains(MapView)) {
      signalMappingVal.put("MapView",mapViewMap(MapView))
    } else {
      logger.info("The value ${MapView} for parameter MapView is not valid.")
    }
    signalMappingVal
  }

  def isMapAutozoom(finput: String, foutPut: String): MutableMap[String, String] = {
    val isZoom = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isZoom == 1 || isZoom == 0 ) {
      signalMappingVal.put("isZoom",isZoom.toString)
    } else {
      logger.info("The value ${isZoom} for parameter isZoom is not valid.")
    }
    signalMappingVal
  }

  //****************<<Speech>>******************************************//

  def sdsStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val statusMap = Map("0" -> "INITIALIZING", "1" -> "BOOTING", "2" -> "LANGUAGE_CHANGE", "3" -> "READY", "4" -> "DIALOG",
      "5" -> "DIALOG_PAUSE", "6" -> "SHUTDOWN", "7" -> "ENGINE_FAILURE", "8" -> "DEVICE_FAILURE", "9" -> "INVALID")
    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val status = signalInputVal(0)
    val clientID = signalInputVal(1)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (statusMap.contains(status)) {
      signalMappingVal.put("status", statusMap(status))
      signalMappingVal.put("clientID", clientID)
    } else {
      logger.info("The value ${status} for parameter status is not valid.")
    }
    signalMappingVal
  }

  //****************<<MapDisplayContent>>******************************************//

  def getTrafficVisibility(finput: String, foutPut: String): MutableMap[String, String] = {

    val trafficVisibilityBitfieldMap = Map("0" -> "NONE", "1" -> "CONGESTION_FLOW", "2" -> "FREE_FLOW", "4" -> "ACCIDENTS", "8" -> "RESTRICTIONS",
      "16" -> "ACTIVITIES", "32" -> "OTHER", "64" -> "INCIDENTS", "255" -> "ALL")
    val TrafficVisibilityBitfield = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (trafficVisibilityBitfieldMap.contains(TrafficVisibilityBitfield)) {
      signalMappingVal.put("TrafficVisibilityBitfield", trafficVisibilityBitfieldMap(TrafficVisibilityBitfield))
    } else {
      logger.info("The value ${TrafficVisibilityBitfield} for parameter TrafficVisibilityBitfield is not valid.")
    }
    signalMappingVal
  }

  def getPOIVisibility(finput: String, foutPut: String): MutableMap[String, String] = {

    val poiVisibilityBitfieldMap = Map("0" -> "NONE", "1" -> "STANDARD", "2" -> "CUSTOM", "4" -> "GAS_STATION", "8" -> "PARKING",
      "16" -> "BANK", "3" -> "RESTAURANT", "64" -> "CONVENIENCE_STORE", "128" -> "HOTEL")
    val POIVisibilityBitfield = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (poiVisibilityBitfieldMap.contains(POIVisibilityBitfield)) {
      signalMappingVal.put("POIVisibilityBitfield", poiVisibilityBitfieldMap(POIVisibilityBitfield))
    } else {
      logger.info("The value ${POIVisibilityBitfield} for parameter POIVisibilityBitfield is not valid.")
    }
    signalMappingVal
  }

  def poiVisibilityChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newPOIVisibilityMap = Map("0" -> "NONE", "1" -> "STANDARD", "2" -> "CUSTOM", "4" -> "GAS_STATION", "8" -> "PARKING",
      "16" -> "BANK", "32" -> "RESTAURANT", "64" -> "CONVENIENCE_STORE", "128" -> "HOTEL")

    val newPOIVisibility = finput
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newPOIVisibilityMap.contains(newPOIVisibility)) {
      signalMappingVal.put("newPOIVisibility", newPOIVisibilityMap(newPOIVisibility))
    } else {
      logger.info("The value ${newPOIVisibility} for parameter newPOIVisibility is not valid.")
    }
    signalMappingVal
  }

  def trafficVisibilityChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newTrafficVisibilityMap = Map("0" -> "NONE", "1" -> "CONGESTION_FLOW", "2" -> "FREE_FLOW", "4" -> "ACCIDENTS", "8" -> "RESTRICTIONS",
      "16" -> "ACTIVITIES", "32" -> "OTHER", "64" -> "INCIDENTS", "255" -> "ALL")

    val newTrafficVisibility = finput
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newTrafficVisibilityMap.contains(newTrafficVisibility)) {
      signalMappingVal.put("newTrafficVisibility", newTrafficVisibilityMap(newTrafficVisibility))
    } else {
      logger.info("The value ${newTrafficVisibility} for parameter newTrafficVisibility is not valid.")
    }
    signalMappingVal
  }

  def fuelRangeVisibilityChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newFuelRangeVisibility = Integer.parseInt(finput)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newFuelRangeVisibility == 0 || newFuelRangeVisibility ==1) {
      signalMappingVal.put("newFuelRangeVisibility", finput)
    } else {
      logger.info("The value ${newFuelRangeVisibility} for parameter newFuelRangeVisibility is not valid.")
    }
    signalMappingVal
  }

  def satelliteMapVisibilityChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newSatelliteMapVisibility = Integer.parseInt(finput)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newSatelliteMapVisibility == 0 || newSatelliteMapVisibility ==1) {
      signalMappingVal.put("newSatelliteMapVisibility", finput)
    } else {
      logger.info("The value ${newSatelliteMapVisibility} for parameter newSatelliteMapVisibility is not valid.")
    }
    signalMappingVal
  }

  def trafficMapStyleChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newTrafficMapStyle = Integer.parseInt(finput)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newTrafficMapStyle == 0 || newTrafficMapStyle ==1) {
      signalMappingVal.put("trafficMapStyleChanged", finput)
    } else {
      logger.info("The value ${newTrafficMapStyle} for parameter newTrafficMapStyle is not valid.")
    }
    signalMappingVal
  }

  def trafficCamVisibilityChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newTrafficVisibility = Integer.parseInt(finput)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newTrafficVisibility == 0 || newTrafficVisibility ==1) {
      signalMappingVal.put("newTrafficVisibility", finput)
    } else {
      logger.info("The value ${newTrafficVisibility} for parameter newTrafficVisibility is not valid.")
    }
    signalMappingVal
  }

  def weatherVisibilityChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newWeatherVisibility = Integer.parseInt(finput)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newWeatherVisibility == 0 || newWeatherVisibility ==1) {
      signalMappingVal.put("newWeatherVisibility", finput)
    } else {
      logger.info("The value ${newWeatherVisibility} for parameter newWeatherVisibility is not valid.")
    }
    signalMappingVal
  }

  def getMapSkinType(finput: String, foutPut: String): MutableMap[String, String] = {

    val mapSkinTypeMap = Map("1" -> "NIGHT", "2" -> "DAY")
    val MapSkinType = foutPut.trim
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (mapSkinTypeMap.contains(MapSkinType)) {
      signalMappingVal.put("MapSkinType", mapSkinTypeMap(MapSkinType))
    } else {
      logger.info("The value ${MapSkinType} for parameter MapSkinType is not valid.")
    }
    signalMappingVal
  }

  def isWeatherVisibility(finput: String, foutPut: String): MutableMap[String, String] = {

    val isVisible = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isVisible == 0 || isVisible ==1) {
      signalMappingVal.put("isVisible", foutPut)
    } else {
      logger.info("The value ${isVisible} for parameter isVisible is not valid.")
    }
    signalMappingVal
  }

  def isFuelRangeVisibility(finput: String, foutPut: String): MutableMap[String, String] = {

    val isVisible = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isVisible == 0 || isVisible ==1) {
      signalMappingVal.put("isVisible", foutPut)
    } else {
      logger.info("The value ${isVisible} for parameter isVisible is not valid.")
    }
    signalMappingVal
  }

  def isSatelliteMapVisibility(finput: String, foutPut: String): MutableMap[String, String] = {

    val isVisible = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isVisible == 0 || isVisible ==1) {
      signalMappingVal.put("isVisible", foutPut)
    } else {
      logger.info("The value ${isVisible} for parameter isVisible is not valid.")
    }
    signalMappingVal
  }

  def isTrafficMapStyle(finput: String, foutPut: String): MutableMap[String, String] = {

    val isVisible = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isVisible == 0 || isVisible ==1) {
      signalMappingVal.put("isVisible", foutPut)
    } else {
      logger.info("The value ${isVisible} for parameter isVisible is not valid.")
    }
    signalMappingVal
  }

  def isTrafficCamVisibility(finput: String, foutPut: String): MutableMap[String, String] = {

    val isVisible = Integer.parseInt(foutPut)
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (isVisible == 0 || isVisible ==1) {
      signalMappingVal.put("isVisible", foutPut)
    } else {
      logger.info("The value ${isVisible} for parameter isVisible is not valid.")
    }
    signalMappingVal
  }

  //****************<<ObjectStorage>>******************************************//

  def getStorageNumberOfEntries(finput: String, foutPut: String): MutableMap[String, String] = {

    val storageIDMap = Map("1" -> "ONBOARD_DB", "13" -> "OFFBOARD_DB", "11" -> "PRESET_DEST", "14" -> "PRESET_TRAFFIC"
      , "3" -> "LAST_DEST",
      "12" -> "EXTERNAL_DEST", "4" -> "PERSONAL", "7" -> "MEMORY_POINTS", "8" -> "TRAFFIC", "10" -> "NAVI")
    val storageID = finput
    val numEntries = foutPut
    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (storageIDMap.contains(storageID)) {
      signalMappingVal.put("storageID", storageIDMap(storageID))
      signalMappingVal.put("numEntries", foutPut)
    } else {
      logger.info("The value ${storageID} for parameter storageID is not valid.")
    }
    signalMappingVal
  }

  def storageStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val storageIDMap = Map("1" -> "ONBOARD_DB", "13" -> "OFFBOARD_DB", "11" -> "PRESET_DEST", "14" -> "PRESET_TRAFFIC"
      , "3" -> "LAST_DEST",
      "12" -> "EXTERNAL_DEST", "4" -> "PERSONAL", "7" -> "MEMORY_POINTS", "8" -> "TRAFFIC", "10" -> "NAVI")
    val StorageStatusMap = Map("1"-> "NOT_PRESENT", "2" -> "LOCKED", "5" -> "INCONSISTENT", "3" -> "PROCESSING", "4" -> "READY")
    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val storageID = signalInputVal(0)
    val StorageStatus = signalInputVal(1)

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (storageIDMap.contains(storageID) && StorageStatusMap.contains(StorageStatus)) {
      signalMappingVal.put("storageID", storageIDMap(storageID))
      signalMappingVal.put("StorageStatus", StorageStatusMap(StorageStatus))
    } else {
      logger.info("The value ${storageID} for parameter storageID is not valid or The value ${StorageStatus} for parameter StorageStatus is not valid.")
    }
    signalMappingVal
  }

  def getStorageStatus(finput: String, foutPut: String): MutableMap[String, String] = {

    val storageIDMap = Map("1" -> "ONBOARD_DB", "13" -> "OFFBOARD_DB", "11" -> "PRESET_DEST", "14" -> "PRESET_TRAFFIC", "3" -> "LAST_DEST",
      "12" -> "EXTERNAL_DEST", "4" -> "PERSONAL", "7" -> "MEMORY_POINTS", "8" -> "TRAFFIC", "10" -> "NAVI")
    val StorageStatusMap = Map("1"-> "NOT_PRESENT", "2" -> "LOCKED", "5" -> "INCONSISTENT", "3" -> "PROCESSING", "4" -> "READY")

    val storageID = finput
    val StorageStatus = foutPut

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (storageIDMap.contains(storageID) && StorageStatusMap.contains(StorageStatus)) {
      signalMappingVal.put("storageID", storageIDMap(storageID))
      signalMappingVal.put("StorageStatus", StorageStatusMap(StorageStatus))
    } else {
      logger.info("The value ${storageID} for parameter storageID is not valid or The value ${StorageStatus} for parameter StorageStatus is not valid.")
    }
    signalMappingVal
  }

  //****************<<Routing>>******************************************//

  def getStorageAvailablePercentage(finput: String, foutPut: String): MutableMap[String, String] = {

    val storageIDMap = Map("1" -> "ONBOARD_DB", "13" -> "OFFBOARD_DB", "11" -> "PRESET_DEST", "14" -> "PRESET_TRAFFIC", "3" -> "LAST_DEST",
      "12" -> "EXTERNAL_DEST", "4" -> "PERSONAL", "7" -> "MEMORY_POINTS", "8" -> "TRAFFIC", "10" -> "NAVI")

    val storageID = finput
    val availableSpace = foutPut

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (storageIDMap.contains(storageID)) {
      signalMappingVal.put("storageID", storageIDMap(storageID))
      signalMappingVal.put("availableSpace", availableSpace)
    } else {
      logger.info("The value ${storageID} for parameter storageID is not valid.")
    }
    signalMappingVal
  }

  //****************<<Guiding>>******************************************//

  def guideStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val guideStatusMap = Map("1" -> "CALCULATING", "2"-> "CALCULATED", "3" -> "NO_GUIDING")
    val GuideStatus = finput

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (guideStatusMap.contains(GuideStatus)) {
      signalMappingVal.put("GuideStatus", guideStatusMap(GuideStatus))
    } else {
      logger.info("The value ${GuideStatus} for parameter GuideStatus is not valid.")
    }
    signalMappingVal
  }

  def guideLevelChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val guideLevelMap = Map("1" -> "QUIET", "2" -> "SHORT", "3" -> "MEDIUM", "4" -> "LONG")
    val GuideLevel = finput

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (guideLevelMap.contains(GuideLevel)) {
      signalMappingVal.put("GuideLevel", guideLevelMap(GuideLevel))
    } else {
      logger.info("The value ${GuideLevel} for parameter GuideLevel is not valid.")
    }
    signalMappingVal
  }

  def getGuideStatus(finput: String, foutPut: String): MutableMap[String, String] = {

    val guideStatusMap = Map("1" -> "CALCULATING", "2"-> "CALCULATED", "3" -> "NO_GUIDING")
    val GuideStatus = foutPut

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (guideStatusMap.contains(GuideStatus)) {
      signalMappingVal.put("GuideStatus", guideStatusMap(GuideStatus))
    } else {
      logger.info("The value ${GuideStatus} for parameter GuideStatus is not valid.")
    }
    signalMappingVal
  }

  def getGuideLevel(finput: String, foutPut: String): MutableMap[String, String] = {

    val guideLevelMap = Map("1" -> "QUIET", "2" -> "SHORT", "3" -> "MEDIUM", "4" -> "LONG")
    val GuideLevel = foutPut

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (guideLevelMap.contains(GuideLevel)) {
      signalMappingVal.put("GuideLevel", guideLevelMap(GuideLevel))
    } else {
      logger.info("The value ${GuideLevel} for parameter GuideLevel is not valid.")
    }
    signalMappingVal
  }

  def mapSkinTypeChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val newMapSkinMap = Map("1" -> "NIGHT", "2" -> "DAY")
    val newMapSkin = finput

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (newMapSkinMap.contains(newMapSkin)) {
      signalMappingVal.put("newMapSkin", newMapSkinMap(newMapSkin))
    } else {
      logger.info("The value ${newMapSkin} for parameter newMapSkin is not valid.")
    }
    signalMappingVal
  }

  //****************<<DestinationPredictionService>>******************************************//

  def resetPredictionModelStatusChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    val statusMap = Map("0" -> "IDLE", "1" -> "IN_PROGRESS", "2" -> "FINISHED", "3" -> "ABORTED", "255" -> "FAILURE")

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val status = signalInputVal(0).trim()
    val clientId = signalInputVal(1).trim()

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (statusMap.contains(status)) {
      signalMappingVal.put("status", statusMap(status))
      signalMappingVal.put("clientId", clientId)
    } else {
      logger.info("The value ${status} for parameter status is not valid.")
    }
    signalMappingVal
  }

  def destinationPredictionEnabledChanged(finput: String, foutPut: String): MutableMap[String, String] = {

    var signalInputVal = new Array[String](2)
    signalInputVal = finput.split(",")
    val enabled = Integer.parseInt(signalInputVal(0).trim())
    val clientId = signalInputVal(1).trim()

    val signalMappingVal = scala.collection.mutable.Map[String, String]()

    if (enabled == 0 || enabled == 1) {
      signalMappingVal.put("enabled", enabled.toString())
      signalMappingVal.put("clientId", clientId)
    } else {
      printf("The value ${enabled} for parameter enabled is not valid.")
    }
    signalMappingVal
  }

}
