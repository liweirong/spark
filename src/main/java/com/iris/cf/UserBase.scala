package com.iris.cf

import breeze.numerics.{pow, sqrt}
import org.apache.spark.sql.SparkSession

object UserBase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("User Base CF").enableHiveSupport()
      .getOrCreate()
    val udata = spark.sql("select * from bigdata.udata")

    import spark.implicits._
    // 1.计算相似用户  cosine = a*b/(|a|*|b|)

    // 所有分母 ,每个用户的打分平方和加起来开根号 |a|= sqrt(a1^2+a2^2+...+an^2) 平方求和开根号
    //    val userScoreSum1 = udata.rdd.map(x => print(x))   0:user_id / 2:rating
    val userScoreSum = udata.rdd.map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(x => sqrt(x.toArray.map(rating => pow(rating.toDouble, 2)).sum)) // sqrt(a1^2+a2^2+...+an^2)
      // userScoreSum.take(10)
      // -> res2: Array[(String, Double)] = Array((273,17.378147196982766), (528,28.160255680657446), (584,17.916472867168917), (736,16.186414056238647), (456,52.40229002629561), (312,66.83561924602779), (62,53.0659966456864), (540,29.866369046136157), (627,46.62617290749907), (317,17.08800749063506))
      .toDF("user_id", "rating_sqrt_sum") // 用户 对应的分母

    // 1.1 item -> user 倒排表

    val df = udata.selectExpr("user_id as user_v", "item_id as item_id", "rating as rating_v") //|user_v|item_id|rating_v|
    val df_decare = udata.join(df, "item_id")
      .filter("cast(user_id as long) <> cast(user_v as long)") // 不等于！排除对角，笛卡儿积也需要，互为相似
    //    |item_id|user_id|rating|timestamp|user_v|rating_v|
    //    +-------+-------+------+---------+------+--------+
    //    |    242|    196|     3|881250949|   721|       3|
    //    |    242|    196|     3|881250949|   720|       4|
    //    |    242|    196|     3|881250949|   500|       3|

    //dot，两个用户对应的商品进行相乘再相加
    import org.apache.spark.sql.functions._
    val product_udf = udf((s1: Int, s2: Int) => s1.toDouble * s2.toDouble)

    // 1.2.1 相乘
    val df_product = df_decare.withColumn("rating_product", product_udf(col("rating"), col("rating_v")))
      .select("user_id", "user_v", "rating_product")

    // 1.2.2相加，计算完整的分子部分
    val df_sim_group = df_product.groupBy("user_id", "user_v") // 对两个用户进行聚合
      .agg("rating_product" -> "sum") // 求和函数
      .withColumnRenamed("sum(rating_product)", "rating_dot") // 默认名字修改

    // ###########--------此时上面会得到两张表 user_id user_v 分子 |  user_id 分母------------###############
    // 1.3 -->  |user_id |user_v |点乘分子 |分母id | 分母v|
    val userScoreSum_v = userScoreSum.selectExpr("user_id as user_v",
      "rating_sqrt_sum as rating_sqrt_sum_v")

    val df_sim = df_sim_group
      .join(userScoreSum, "user_id")
      .join(userScoreSum_v, "user_v") // 把分母join进来
      .selectExpr("user_id", "user_v",
      "rating_dot / (rating_sqrt_sum * rating_sqrt_sum_v) as cosine_sim") // 余弦相似度

    //    |user_v|rating_v|cosine_sim  |
    //    +------+--------+------------+
    //    |   125|     296| 0.271716124|


    /** 至此对应的用户相似度已经计算好了 */

    //  2.  获取相似用户的物品集合
    //    2.1取得前n个相似用户
    val df_nsim = df_sim.rdd.map(x => (x(0).toString, (x(1).toString, x(2).toString))) // user_id（user_v ,分数）
      .groupByKey().mapValues { x =>
      x.toArray.sortWith((x, y) => x._2 > y._2).slice(0, 10) // 降序取十个  列转行
    }.flatMapValues(x => x).toDF("user_id", "user_v_sim") // 行转列
      .selectExpr("user_id", "user_v_sim._1 as user_v", "user_v_sim._2 as sim")

    // 2.2获取用户的物品集合进行过滤 ：user_id [item_id _ rating, ...]    273|[328_3, 345_3, 31...|
    val df_user_item = udata.rdd.map(x => (x(0).toString, x(1).toString + "_" + x(2).toString))
      .groupByKey().mapValues(x => x.toArray)
      .toDF("user_id", "item_rating_arr")
    // 过滤 user_id | item_rating_arr | user_v | item_rating_arr_v
    val df_user_item_v = df_user_item.selectExpr("user_id as user_v",
      "item_rating_arr as item_rating_arr_v")
    //  分别为user_id和user_v携带items进行过滤

    /** df_gen_item:
      * +------+-------+-------------------+--------------------+--------------------+
      * |user_v|user_id|                sim|     item_rating_arr|   item_rating_arr_v|
      * +------+-------+-------------------+--------------------+--------------------+
      * |   296|     71|0.33828954632615976|[89_5, 134_3, 346...|[705_5, 508_5, 20...|
      * |   467|     69| 0.4284583738949647|[256_5, 240_3, 26...|[1017_2, 50_4, 15...|
      * |   467|    139|0.32266158985444504|[268_4, 303_5, 45...|[1017_2, 50_4, 15...|
      * |   467|    176|0.44033327143526596|[875_4, 324_5, 32...|[1017_2, 50_4, 15...|
      * |   467|    150|0.47038691576507874|[293_4, 181_5, 12...|[1017_2, 50_4, 15...|
      * +------+-------+-------------------+--------------------+--------------------+
      * */
    val df_gen_item = df_nsim.join(df_user_item, "user_id")
      .join(df_user_item_v, "user_v")

    /**
      *   2.3用一个udf过滤相似用户user_id1中包含user_id已经打过分的物品 = > item_rating_arr_v - item_rating_arr
      */
    val filter_udf = udf { (items: Seq[String], items_v: Seq[String]) =>
      val fMap = items.map { x =>
        val l = x.split("_")
        (l(0), l(1))
      }.toMap // 物品和打分的map集合{89=>5, 134=>3}

      items_v.filter { x =>
        val l = x.split("_")
        fMap.getOrElse(l(0), -1) == -1 // 过滤掉user_id重复的物品，取map中没有的
      }
    }
    val df_filter_item = df_gen_item.withColumn("filtered_item",
      filter_udf(col("item_rating_arr"), col("item_rating_arr_v")))
      .select("user_id", "sim", "filtered_item") // user_v此时得到了集合和相似度已经不需要了

    /** df_filter_item:  user_id | user_v(此时不再需要)  | 用户相似度  | 物品_打分...|
      * +-------+-------------------+--------------------+
      * |user_id|                sim|       filtered_item|
      * +-------+-------------------+--------------------+
      * |     71|0.33828954632615976|[705_5, 508_5, 20...|
      * |     69| 0.4284583738949647|[762_3, 264_2, 25...|
      * |    139|0.32266158985444504|[1017_2, 50_4, 76...|
      * |    176|0.44033327143526596|[1017_2, 762_3, 2...|
      * |    150|0.47038691576507874|[1017_2, 762_3, 2...|
      * +-------+-------------------+--------------------+
      * */


    // 2.4 公式计算 相似度*rating
    val simRatingUDF = udf { (sim: Double, items: Seq[String]) =>
      items.map { x =>
        val l = x.split("_")
        l(0) + "_" + l(1).toDouble * sim
      }
    }

    val itemSimRating = df_filter_item.withColumn("item_prod",
      simRatingUDF(col("sim"), col("filtered_item")))
      .select("user_id", "item_prod")

    /** itemSimRating:
      * +-------+--------------------+
      * |user_id|           item_prod|
      * +-------+--------------------+
      * |     71|[705_1.6914477316...|
      * |     69|[762_1.2853751216...|
      * |    139|[1017_0.645323179...|
      * |    176|[1017_0.880666542...|
      * |    150|[1017_0.940773831...|
      * +-------+--------------------+
      */


    // 2.5 进行 行专列
    val userItemScore = itemSimRating.select(itemSimRating("user_id"),
      explode(itemSimRating("item_prod")))
      .toDF("user_id", "item_prod")
      .selectExpr("user_id", "split(item_prod,'_')[0] as item_id",
        "cast(split(item_prod,'_')[1] as double) as score")

    /** userItemScore:
      * +-------+-------+------------------+
      * |user_id|item_id|             score|
      * +-------+-------+------------------+
      * |     71|    705|1.6914477316307988|
      * |     71|    508|1.6914477316307988|
      * |     71|     20|1.6914477316307988|
      * |     71|    228| 1.353158185304639|
      * |     71|    855|1.6914477316307988|
      * +-------+-------+------------------+
      */

    // ------ 处理完毕 --------
    userItemScore.show()
  }
}
