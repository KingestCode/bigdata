package com.rox.dmp.tags

trait Tags {

  /**
    * 定义打标签的方法接口
    * @param args 任意类型 可变参数
    * @return 返回 map : String -> Int
    */
  def makeTags(args: Any*): Map[String, Int]

}
