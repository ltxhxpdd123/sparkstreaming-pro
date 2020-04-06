package com.ltxhxpdd.exactly

import java.sql.DriverManager

import com.ltxhxpdd.Config

object Test {
  def main(args: Array[String]): Unit = {
    val dbConn = DriverManager.getConnection(Config.jdbcUrl, Config.jdbcUser, Config.jdbcPassword)
    dbConn.close()
  }

}
