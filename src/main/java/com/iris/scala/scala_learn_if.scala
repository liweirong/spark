package com.iris.scala

/**
  * 条件表达式
  */
object scala_learn_if {
  def main(args: Array[String]): Unit = {
    val i = 1
    val m = if (i > 0) 100 else 0
    val v = {
      if (i > 0) {
        100
      } else {
        0
      }
    }
    System.out.println("条件表达式：" + m, v) //(条件表达式100,100)

    val n = if (i > 2) 1 else ()
    print("缺失else返回值：" + n) //缺失else返回值：() -void

    val z: Any = if (i > 0) 1 else "error"
    System.out.println("支持混合类型表达式：" + z) // i>2 支持混合类型表达式：error
    val it:Int = z.asInstanceOf[Int] //转成Int类型
    println(it.isInstanceOf[Int])

  }
}
