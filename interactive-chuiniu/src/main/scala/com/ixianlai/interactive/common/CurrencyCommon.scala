package com.ixianlai.interactive.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

object CurrencyCommon {
    def selectOdsBrand(spark: SparkSession)(day: String,
                                            ods_db_usercenter_hird_brand_bind: String,
                                            edw_server_loginlogout: String,
                                            edw_gameover_user: String,
                                            edw_gameover_app: String) = {
        import spark.implicits._
        // 每日绑定吹牛的用户
        // app_id user_id
        val temp1 = spark.read.table(ods_db_usercenter_hird_brand_bind).where(s"brand='chuiniu' and target_day=$day")
                .select($"xlhu_appid".as("app_id"), $"xlhu_userid".as("user_id")).distinct()

        // 用户登录登出
        // target_day app_id user_id
        val temp2 = spark.read.table(edw_server_loginlogout).where(s"target_day=$day and (plugin_id = 0 or plugin_id is null) and (login_count>0 or logout_count>0)")
                .select($"target_day".cast("STRING"), $"app_id", $"user_id").distinct()

        // 登录用户中绑定吹牛的用户
        // target_day app_id user_id
        val temp3 = temp1.join(temp2, Seq("app_id", "user_id"))
                .select(temp2.col("target_day"), temp1.col("app_id"), temp1.col("user_id"))

        // 结果一,登录用户绑定吹牛的用户总数
        val res1 = temp3.agg(count("*").as("all_count")).select($"all_count")
                .withColumn("target_day", when($"all_count" === 1, s"$day").otherwise("$day"))

        // 亲友圈和好友桌完整把数>=1的用户
        val temp4 = spark.read.table(edw_gameover_user).where("dissolved=false and bs_play>=1").where($"target_day".cast("STRING") === s"$day")
                .select($"app_id", $"user_id", $"guild_id")

        // A中亲友圈和好友桌完整把数>=1的人数
        val temp5 = temp3.join(temp4, Seq("app_id", "user_id"))
                .select(temp3.col("target_day"), temp3.col("app_id"), temp3.col("user_id"), temp4.col("guild_id"))
                .withColumn("new_guild_id", when($"guild_id">0, 1).otherwise(0))

        // A中亲友圈和好友桌完整把数>=1的用户数
        val res2 = temp5.where($"new_guild_id" === 1)
                .agg(count("*").as("qinyouquan_count")).select($"qinyouquan_count")
                .withColumn("target_day", when($"qinyouquan_count" === 1, s"$day").otherwise("$day"))

        val res4 = temp5.where($"new_guild_id" === 0)
                .agg(count("*").as("haoyouzhuo_count")).select($"haoyouzhuo_count")
                .withColumn("target_day", when($"haoyouzhuo_count" === 1, s"$day").otherwise("$day"))

        val res = res1.join(res2, "target_day").join(res4, "target_day")
                .select(res1.col("all_count"), res2.col("qinyouquan_count"), res4.col("haoyouzhuo_count"))
        // res.rdd.map(_.mkString("|")).saveAsTextFile("file:")
        res.write.format("csv").save("file:///home/hadoop/liuhui/output")
    }
}
