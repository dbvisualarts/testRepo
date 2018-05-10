package com.datamantra.sensordataprocessor.msgprocessor

/**
  * Created by SUDREGO on 8/3/2017.
  */

case class vcdMsgFormat(
                         uuID : String,
                         pv : String,
                         ssn : Int,
                         tsn : Int,
                         EVENT_REC_TIME : java.sql.Timestamp,
                         SIGNAL_NAME : String,
                         SIGNAL_DESC : String,
                         SIGNAL_CODE : String,
                         SIGNAL_VALUE : String,
                         CREATE_DT : java.sql.Timestamp
                       )