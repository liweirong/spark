package com.iris.offline.feat

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object SimpleFeature {
  def Feat(priors:DataFrame,orders:DataFrame):(DataFrame,DataFrame)={
    /**product feature
      * 1.销售量 prod_cnt
      * 2.商品被再次购买（reordered）量prod_sum_rod
      * 3.统计reordered比率 prod_rod_rate
      * */
//    prod_cnt
    val prodCnt = priors.groupBy("product_id").count()
//    sum(reordered) ,统计reordered比率
    val prodRodCnt = priors.selectExpr("product_id","cast(reordered as int)")
      .groupBy("product_id")
      .agg(sum("reordered").as("prod_sum_rod"),
        avg("reordered").as("prod_rod_rate"),
        count("product_id").as("prod_cnt"))

    /**
      * user Features:
      * 1. 每个用户购买订单的平均间隔
      * 2. 每个用户的总订单数
      * 3. 每个用户购买的product商品去重后的集合数据
      * 4. 用户总商品数量以及去重后的商品数量
      * 5. 每个用户购买的平均每个订单商品数量
      * */
//      异常值处理：将days_since_prior_order中的空值进行处理
    val ordersNew = orders
      .selectExpr("*","if(days_since_prior_order='',0,days_since_prior_order) as dspo")
      .drop("days_since_prior_order")
//    1.每个用户平均购买订单的间隔周期：avg（dspo）
    val userGap = ordersNew.selectExpr("user_id","cast(dspo as int)")
      .groupBy("user_id").avg("dspo")
      .withColumnRenamed("avg(dspo)","u_avg_day_gap")
//     2. 每个用户的总订单数
    val userOrdCnt = orders.groupBy("user_id").count()
//      3.每个用户购买的product商品驱虫后的集合
    val op = orders.join(priors,"order_id").select("user_id","product_id")

//    RDD转DataFrame：需要隐式转换toDF
    import priors.sparkSession.implicits._
    //collect_set
    val userUniOrdRecs = op.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey()
      .mapValues(_.toSet.mkString(","))
      .toDF("user_id","product_records")

//    4. 用户总商品数量以及去重后的商品数量
    val userProRcdSize = op.rdd.map(x=>(x(0).toString,x(1).toString))
      .groupByKey().mapValues{record=>
      val rs = record.toSet
      (rs.size,rs.mkString(","))
    }.toDF("user_id","tuple")
      .selectExpr("user_id","tuple._1 as prod_dist_cnt","tuple._2 as prod_records")

//    5. 每个用户购买的平均每个订单商品数量
//    1)先求每个订单的商品数量【对order做聚合count（）】
    val ordProCnt = priors.groupBy("order_id").count()
//    ordProCnt.rdd.map(x=>(x(0).toString,x(1).toString)).collectAsMap()
//    2)求每个用户订单中商品个数的平均值【对user做聚合，avg(商品个数)】
    val userPerOrdProdCnt = orders.join(ordProCnt,"order_id")
      .groupBy("user_id")
      .avg("count").withColumnRenamed("avg(count)","u_avg_ord_prods") //.persist(StorageLevel.MEMORY_ONLY)
    val userFeat = userGap.join(userOrdCnt,"user_id")
      .join(userProRcdSize,"user_id")
      .join(userPerOrdProdCnt,"user_id")
      .selectExpr("user_id",
        "u_avg_day_gap",
        "count as user_ord_cnt",
        "prod_dist_cnt as u_prod_dist_cnt",
        "prod_records as u_prod_records",
        "u_avg_ord_prods")

    /**
      * user and product Feature: cross feature 交叉特征
      * 1. 统计user和对应product在多少个订单中出现（distinct order_id）
      * 2. 特定product具体在购物车中的出现位置的平均位置
      * 3. 最后一个订单id
      * 4. 用户对应product在所有这个用户购买产品量中的占比rate
      * */

    (prodRodCnt,userFeat)
  }

}
