package com.rox.dmp.utils

object RptUtils {

  /**
    * return:  List(原始请求数,有效请求数,广告请求数)
    */
  def calculateReq(reqMode:Int, prcNode:Int): List[Double] = {
    if (reqMode==1 && prcNode==1){
      List(1,0,0)
    }else if (reqMode==1 && prcNode==2){
      List(1,1,0)
    }else if (reqMode==1 && prcNode==3){
      List(1,1,1)
    }else List(0,0,0)
  }


  /**
    * return:  List(竞价成功数,展示数,DSP广告消费,DSP广告成本)
    */
  def calculateRtb(effTive: Int, bill: Int, bid: Int, orderId: Int, win: Int, winPrice: Double, adPayMent: Double): List[Double] ={

    if (effTive == 1 && bill == 1 && bid == 1 && orderId != 0) {
      List[Double](1, 0, 0, 0)
    } else if (effTive == 1 && bill == 1 && win == 1) {
      List[Double](0, 1, winPrice / 1000.0, adPayMent / 1000.0)
    } else List[Double](0, 0, 0, 0)
  }


  /**
    * return:  List(展示数,点击数)
    */
  def calculateShowClick(reqMode: Int, effTive: Int): List[Double] = {
    if (reqMode == 2 && effTive == 1) {
      List[Double](1, 0)
    } else if (reqMode == 3 && effTive == 1) {
      List[Double](0, 1)
    } else List[Double](0, 0)
  }

}
