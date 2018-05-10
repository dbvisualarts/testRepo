package com.datamantra.sensordataprocessor.msgprocessor

/**
  * Created by RHARIKE on 9/9/2017.
  */
case class stfMsgFormat (
  CONT_ID : String,
  uuID : String,
  pv : String,
  ssn : Int,
  tsn : Int,
  EVENT_REC_TIME : java.sql.Timestamp,
  FUNC_IDENTIFIER_ADDRESS : String,
  FUNC_GROUP : String,
  FUNC_IDENTIFIER_FUNCTION_NAME : String,
  FUNC_INPUT : String,
  FUNC_OUTPUT : String,
  CREATE_DT : java.sql.Timestamp
)
