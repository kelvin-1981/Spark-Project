package com.yykj.spark.streaming.samples.utils

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

object JDBCUtil {
    //初始化连接池
    var dataSource: DataSource = init()

    //初始化连接池方法
    def init(): DataSource = {
        val properties = new Properties()
        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
        properties.setProperty("url", "jdbc:mysql://node21:3306/spark_streaming_samples?useUnicode=true&characterEncoding=UTF-8")
        properties.setProperty("username", "root")
        properties.setProperty("password", "hh96n55g")
        properties.setProperty("maxActive", "50")
        //德鲁伊
        DruidDataSourceFactory.createDataSource(properties)
    }

    //获取MySQL连接
    def getConnection: Connection = {
        dataSource.getConnection
    }

    //执行SQL语句,单条数据插入
    def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
        var rtn = 0
        var pstmt: PreparedStatement = null
        try {
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)

            if (params != null && params.length > 0) {
                for (i <- params.indices) {
                    pstmt.setObject(i + 1, params(i))
                }
            }
            rtn = pstmt.executeUpdate()
            connection.commit()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }

    def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
        var flag: Boolean = false
        var pstmt: PreparedStatement = null
        try {
            pstmt = connection.prepareStatement(sql)
            for (i <- params.indices) {
                pstmt.setObject(i + 1, params(i))
            }
            flag = pstmt.executeQuery().next()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        flag
    }
}
