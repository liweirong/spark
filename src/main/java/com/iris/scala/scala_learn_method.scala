package com.iris.scala

object scala_learn_method {
  def main(args: Array[String]): Unit = {
    val j = 1.+(2)
    println(j) //3

    println(m(1, 2))
    println($plus(2, 4)) //系统“+”和下面的“+”的方法
    println(add(1, 3))
  }

  //定义方法体
  def m(x: Int, y: Int): Int = {
    x + y
  }

  //和系统的+方法重复，重载
  def +(x: Int, y: Int): Int = {
    x + y
  }

  //省略写法，不写返回值类型-> 自动推断,！！但递归必须写返回值类型
  def add(x: Int, y: Int) = x + y
}
