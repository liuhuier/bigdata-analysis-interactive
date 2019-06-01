package com.ixianlai.interactive.common

import org.apache.spark.sql.{DataFrame, SparkSession}

object CurrencyCommon {
    def selectOdsBrand(spark: SparkSession)(ods_db_usercenter_hird_brand_bind: String,
                                            edw_server_loginlogout: String) = {
        val frame: DataFrame = spark.read.table(ods_db_usercenter_hird_brand_bind)
        
    }
}
