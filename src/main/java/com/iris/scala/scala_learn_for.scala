package com.iris.scala

object scala_learn_for {
  def main(args: Array[String]): Unit = {

    for (i <- 1 to 10) print(i)
    for (i <- 1 until 10) print(i)

    print(1 to 10) //集合，取出集合[1,10]的值给i Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    print(1 until 10) //集合，取出集合[1,10)的值给i Range(1, 2, 3, 4, 5, 6, 7, 8, 9)

    val ch = "helloWorld"
    for (x <- "str") print(x)
    for (x <- 0 until ch.length) print(ch.charAt(x)) // helloWorld

    val arr = Array(1, 3, 4, 5, 6, 7) //数组
    for (x <- arr) print(x) // 134567
    for (x <- 0 until arr.length) print(arr(x)) // 134567

    println()
    //高级for循环
    for (i <- 1 to 3; j <- 1 to 3 if i != j)
      println(10 * i + j)
    println()

    /**
      * 12
      * 13
      * 21
      * 23
      * 31
      * 32
      */

    //for 推导式:如果for循环的循环体以yield开始，则该循环体会构建出一个集合，每次迭代生成该集合中的一个值
    for (i <- arr) yield i * 10
    val ints = arr.map(_ * 10)
    arr.map(x => x * 10)
    for (i <- 0 until ints.length) print(ints(i) + ",") //10,30,40,50,60,70,
    println()
    val ints2 = arr.filter(m => m % 2 == 0) //取偶数 4,6,
      .map(n => n * 1000) // 4000,6000,
    for (i <- 0 until ints2.length) print(ints2(i) + ",")
  }
}
