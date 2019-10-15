package dev

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import dev.config.{Config, ConfigFileParser, DataLakeConfig}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 *  Main class of the project.
 *  TODO (user) : fill in the ADLS credentials and other things specified between < >
 *
 *   @author Inmapg
 */
object Main {
    
  val logger: Logger = LoggerFactory.getLogger(Main.getClass)
  
  def main(args: Array[String]): Unit = {
    val dataLakeConfig =  DataLakeConfig()

    implicit val spark = SparkSession.builder()
      .config(dataLakeConfig)
      .appName("data-lake-compaction")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.<your-storage-account-name>.dfs.core.windows.net",
      "<your-data-lake-password>")
    spark.sparkContext.hadoopConfiguration.set("fs.defaultFS", "abfss://<your- file-system-name>@<your-storage-account-name>" +
      ".dfs.core.windows.net")

    val argsConfigFile = Arguments.parse(args) // Taking configuration file path from command line
    val configFile = spark.read.textFile("abfss://<your-file-system-name>@<your-storage-account-name>.dfs.core.windows.net/"
      + argsConfigFile.configFileName).collect()
    val config = ConfigFileParser.parse(configFile)

    val pathsList = new Array[String](config.filter.length)
    for (i <- config.filter.indices){
      pathsList(i) = config.originPath + "/" + config.filter(i).mkString("/")
    }
    try{
      compaction(config, pathsList)
    } catch{
      case e : Exception => logger.error("Error during compaction")
    }
    spark.close()
  }

  def compaction(config : Config, pathsList : Array[String])(implicit spark: SparkSession): Unit ={
    implicit val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    for (i <- config.filter.indices){
      depthCompaction(pathsList(i) + "/", config.size)
    }
  }

  def depthCompaction(root: String, configSize : Int)(implicit fs : FileSystem, spark : SparkSession): Unit = {
    var newFolder = true
    val statusList = fs.listStatus(new Path(root))
    var i = 0
    while(i < statusList.length && newFolder){ // Not using foreach intentionally to avoid looping over all parquet files
      val compacting = Try {
        val path = statusList(i).getPath().toString
        if(statusList(i).isDirectory){
          depthCompaction(path, configSize)
        }
        else{
          newFolder = false
          val size = fs.getContentSummary(new Path(path)).getLength
          val fileNum = size/(configSize * 1024 * 1024) + 1
          val df = spark.read.parquet(path + "/")
          val pathTmp = path.substring(0, path.lastIndexOf('/')) + "-tmp"
          val aux = new Path(path.substring(0, path.lastIndexOf('/')))
          val auxTmp = new Path(pathTmp)
          fs.mkdirs(auxTmp)
          df.repartition(fileNum.toInt).write.mode(SaveMode.Append).parquet(pathTmp)
          fs.delete(aux, true)
          fs.rename(auxTmp, aux)
        }
      }
      compacting match {
        case Success(s) => {
          // OK
        }
        case Failure(e) => logger.error(e.getMessage)
      }
      i += 1
    }
  }

}
