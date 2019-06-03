package com.ixianlai.interactive.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object CurrencyCommon {
    def selectOdsBrand(spark: SparkSession)(day: String,
                                            ods_db_usercenter_hird_brand_bind: String,
                                            edw_server_loginlogout: String,
                                            edw_gameover_user: String,
                                            edw_gameover_app: String) = {
        // 吹牛用户绑定
        // col("name").as("name1")
        val temp1 = spark.read.table(ods_db_usercenter_hird_brand_bind)
                .where(s"brand='chuiniu' and target_day=$day")
                .select(col("xlhu_appid").as("app_id"), col("xlhu_userid").as("user_id")).distinct()
        // 用户登录登出
        val temp2 = spark.read.table(edw_server_loginlogout)
                .where(s"target_day=$day and (login_count>0 or logout_count>0)")
                .select(col("app_id"), col("user_id"))
                .distinct()
        val temp3 = temp1.join(temp2, Seq("app_id", "user_id"), "inner").select("app_id", "user_id")
        val res1 = temp3.agg(count("*").as("all_count")).select(col(s"${day}").as("target_day"), col("all_count"))
        val temp4 = spark.read.table(edw_gameover_user).where(s"target_day=$day and dissloved=false and bs_play>=1")
                .select("app_id", "user_id", "guild_id")
        val temp5 = temp3.join(temp4, Seq("app_id", "user_id")).select("app_id", "user_id", "guild_id")

        // 好友牌局结束 -- 玩家维度 datasystem.edw_gameover_user
        spark.read.table(edw_gameover_user).createOrReplaceTempView("gameover_user")
        // 好友牌局结束 -- app维度 datasystem.edw_gameover_app
        spark.read.table(edw_gameover_app).createOrReplaceTempView("gameover_app")

    }
}
