package com.yykj.spark.framework.common

trait TDao {

  def readFile(path: String): Any

  def readHdfs(path: String): Any

  def readMySQL(server: String, port: String, user: String, pwd: String): Any

  def readOracle(server: String, port: String, user: String, pwd: String): Any
}
