package dev.config

import org.apache.spark.SparkConf

/**
 * Establishes the spark connection with ADLS
 * @author Inmapg
 */
object DataLakeConfig {
  def apply(): SparkConf  = {

    val sc = new SparkConf()
    sc.set(
      "fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net",
      "<your-data-lake-password>")

    sc
  }
}

