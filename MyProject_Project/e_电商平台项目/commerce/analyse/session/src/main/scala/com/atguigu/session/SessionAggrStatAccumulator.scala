/*
 * Copyright (c) 2018. Atguigu Inc. All Rights Reserved.
 */

package com.atguigu.session

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

/**
  * 自定义累加器
  * 继承AccumulatorV2
  * 累加器本质就是一个 map 中维护者不同的 (k,v), 每传进来一个k, 就取出v, 再+1
  */
class SessionAggrStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {

  // 保存所有聚合数据
  private val aggrStatMap = mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    aggrStatMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val newAcc = new SessionAggrStatAccumulator
    aggrStatMap.synchronized{
      newAcc.aggrStatMap ++= this.aggrStatMap
    }
    newAcc
  }

  override def reset(): Unit = {
    aggrStatMap.clear()
  }

  override def add(v: String): Unit = {
    if (!aggrStatMap.contains(v)){
      aggrStatMap += (v -> 0)
    }
    aggrStatMap.update(v, aggrStatMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    /**
      * foldleft
      * (0/:(1 to 100))(_+_)
      * (0/:(1 to 100)){case (item1,item2) => item1+item2 }
      * 等价于
      * (1 to 100).foldleft(0)(_+_)
      */

    other match {
      case acc:SessionAggrStatAccumulator => {
        (this.aggrStatMap /: acc.value){
          case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )}
      }
    }
  }

  override def value: mutable.HashMap[String, Int] = {
    this.aggrStatMap
  }
}
