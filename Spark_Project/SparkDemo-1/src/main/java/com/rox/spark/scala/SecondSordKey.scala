package com.rox.spark.scala

class SecondSordKey(val first:Int, val second: Int) extends Ordered[SecondSordKey] with Serializable {

  override def compare(that: SecondSordKey): Int = {

    if(this.first - that.first != 0){
      this.first - that.first
    }else {
      this.second - that.second
    }
  }
}
