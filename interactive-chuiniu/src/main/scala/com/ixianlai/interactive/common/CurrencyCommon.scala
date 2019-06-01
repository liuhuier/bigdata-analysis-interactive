package com.ixianlai.interactive.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object CurrencyCommon {
    def selectOdsBrand(spark: SparkSession)(ods_db_usercenter_hird_brand_bind: String,
                                            edw_server_loginlogout: String) = {
        // 吹牛用户绑定
        spark.read.table(ods_db_usercenter_hird_brand_bind).createOrReplaceTempView("userOds")
        // 用户登录登出
        spark.read.table(edw_server_loginlogout).createOrReplaceTempView("userLoginLogout")

    }
}
