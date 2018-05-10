package com.datamantra.sensordataprocessor.utils

/**
  * Created by RHARIKE on 9/20/2017.
  */

import com.datamantra.sensordataprocessor.msgprocessor.{stfMsgFormat, seMsgFormat, vcdMsgFormat}
import org.apache.spark.AccumulatorParam

object VCDCustListAccumulator extends AccumulatorParam[List[vcdMsgFormat]]  {

  override def addInPlace(r1: List[vcdMsgFormat], r2: List[vcdMsgFormat]): List[vcdMsgFormat] = {
    r1 ++ r2
  }

  override def zero(initialValue: List[vcdMsgFormat]): List[vcdMsgFormat] = {
    Nil
  }
}

object SECustListAccumulator extends AccumulatorParam[List[seMsgFormat]]  {

  override def addInPlace(r1: List[seMsgFormat], r2: List[seMsgFormat]): List[seMsgFormat] = {
    r1 ++ r2
  }

  override def zero(initialValue: List[seMsgFormat]): List[seMsgFormat] = {
    Nil
  }
}

object STFCustListAccumulator extends AccumulatorParam[List[stfMsgFormat]]  {

  override def addInPlace(r1: List[stfMsgFormat], r2: List[stfMsgFormat]): List[stfMsgFormat] = {
    r1 ++ r2
  }

  override def zero(initialValue: List[stfMsgFormat]): List[stfMsgFormat] = {
    Nil
  }
}

