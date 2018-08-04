package com.rox.spark.scala.SelfDefineSort_自定义排序

class SecondSordKey(val first:Int, val second: Int) extends Ordered[SecondSordKey] with Serializable {

  override def compare(that: SecondSordKey): Int = {

    if(this.first - that.first != 0){
      this.first - that.first
    }else {
      this.second - that.second
    }
  }
}
