package com.ixianlai.interactive.analysis

import com.ixianlai.interactive.common.CurrencyCommon
import org.apache.spark.sql.SparkSession

/**
  * 吹牛用户行为分析
  */
object ChuiNiuAction {
    def main(args: Array[String]): Unit = {
        val Array(day, ods_db_usercenter_hird_brand_bind, edw_server_loginlogout, edw_gameover_user, edw_gameover_app) = args
        val spark: SparkSession = SparkSession.builder().appName(s"liuhui_ChuiNiuAction_$day").enableHiveSupport().getOrCreate()
        CurrencyCommon.selectOdsBrand(spark)(day, ods_db_usercenter_hird_brand_bind, edw_server_loginlogout, edw_gameover_user, edw_gameover_app)
    }
}
