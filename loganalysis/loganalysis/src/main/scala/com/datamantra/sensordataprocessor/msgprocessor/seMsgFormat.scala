package com.datamantra.sensordataprocessor.msgprocessor

/**
  * Created by RHARIKE on 9/9/2017.
  */
case class seMsgFormat (
                         uuID : String,
                         pv : String,
                         ssn : Int,
                         tsn : Int,
                         EVENT_REC_TIME : java.sql.Timestamp,
                         EVENTID : Int,
                         ENUM : String,
                         EVENT : String,
                         EVENTDATA : String,
                         CREATE_DT : java.sql.Timestamp
                       )

