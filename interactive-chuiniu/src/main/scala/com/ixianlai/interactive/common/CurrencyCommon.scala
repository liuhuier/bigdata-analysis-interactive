package com.ixianlai.interactive.common

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object CurrencyCommon {
    def selectOdsBrand(spark: SparkSession)(day: String,
                                            ods_db_usercenter_hird_brand_bind: String,
                                            edw_server_loginlogout: String,
                                            edw_gameover_user: String,
                                            edw_gameover_app: String) = {
        import spark.implicits._
        // 每日绑定吹牛的用户
        // app_id user_id
        val temp1 = spark.read.table(ods_db_usercenter_hird_brand_bind).where($"brand" === "chuiniu").where($"target_day" === s"$day")
                .select($"xlhu_appid".as("app_id"), $"xlhu_userid".as("user_id")).distinct()

        // 用户登录登出
        // target_day app_id user_id
        val temp2 = spark.read.table(edw_server_loginlogout).where($"target_day" === s"$day")
                .where("(plugin_id = 0 or plugin_id is null) and (login_count>0 or logout_count>0)")
                .select($"target_day".cast("string"), $"app_id", $"user_id").distinct()

        // 登录用户中绑定吹牛的用户[A]
        // target_day app_id user_id
        val temp3 = temp1.join(temp2, Seq("app_id", "user_id")).select(temp2.col("target_day"), temp1.col("app_id"), temp1.col("user_id"))

        // 结果一,登录用户绑定吹牛的用户总数[A]
        // all_count target_day
        val res1 = temp3.agg(count("*").as("all_count"))
                .withColumn("target_day", when($"all_count" === 1, s"$day").otherwise(s"$day"))

        // 亲友圈和好友桌完整把数>=1的用户
        // app_id user_id guild_id
        val temp4 = spark.read.table(edw_gameover_user)
                .where("dissolved=false and bs_play>=1")
//                .where($"dissolved" === false)
//                .where($"bs_play" >= 1)
                .where($"target_day".cast("string") === s"$day")
                .select($"app_id", $"user_id", $"guild_id")

        // A中亲友圈和好友桌完整把数>=1的用户
        // target_day app_id user_id guild_id category
        val temp5 = temp3.join(temp4, Seq("app_id", "user_id"))
                .select(temp3.col("target_day"), temp3.col("app_id"), temp3.col("user_id"), temp4.col("guild_id"))
                .withColumn("category", when($"guild_id" > 0, 1).otherwise(0))

        // A中亲友圈和好友桌完整把数>=1的用户数
        // qinyouquan_count target_day
        var res2 = temp5.where($"category" === 1)
                .agg(count("*").as("qinyouquan_count"))
                .withColumn("target_day", when($"qinyouquan_count" === 1, s"$day").otherwise(s"$day"))

        // haoyouzhuo_count target_day
        var res4 = temp5.where($"category" === 0)
                .agg(count("*").as("haoyouzhuo_count"))
                .withColumn("target_day", when($"haoyouzhuo_count" === 1, s"$day").otherwise(s"$day"))

        // 亲友圈和好友桌总完整把数
        // app_id bs_play guild_id category
        val temp6 = spark.read.table(edw_gameover_app)
                .where($"target_day".cast("string") === s"$day")
                .where($"dissolved" === false)
                .select($"app_id", $"bs_play", $"guild_id")
                .withColumn("category", when($"guild_id" > 0, 1).otherwise(0))

        // app_id
        val temp7 = temp4.groupBy($"app_id").agg(count("*").as("counts")).select($"app_id")
        // bs_play category
        val temp8 = temp7.join(temp6, Seq("app_id")).select(temp6.col("bs_play"), temp6.col("category"))
        // qinyouquan_bashu_count target_day
        var res3 = temp8.where($"category" === 1)
                .agg(sum($"bs_play").as("qinyouquan_bashu_count"))
                .withColumn("target_day", when($"qinyouquan_bashu_count" === 1, s"$day").otherwise(s"$day"))
        // haoyouzhuo_bashu_count target_day
        var res5 = temp8.where($"category" === 0)
                .agg(sum($"bs_play").as("haoyouzhuo_bashu_count"))
                .withColumn("target_day", when($"haoyouzhuo_bashu_count" === 1, s"$day").otherwise(s"$day"))

        val res = res1.join(res2, Seq("target_day")).join(res3, Seq("target_day")).join(res4, Seq("target_day")).join(res5, Seq("target_day"))
                .select(res1.col("target_day"), $"all_count", $"qinyouquan_count", $"qinyouquan_bashu_count", $"haoyouzhuo_count", $"haoyouzhuo_bashu_count")

        res.rdd.map(_.mkString("|")).saveAsTextFile("file:///home/hadoop/liuhui/output")
        spark.stop()
    }
}
