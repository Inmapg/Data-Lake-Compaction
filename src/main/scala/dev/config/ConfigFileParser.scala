package dev.config

import java.util

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._

case class Config(size: Int, originPath: String, filter: Array[Array[String]])

/**
 * Configuration file parser
 * @author Inmapg
 */
object ConfigFileParser {

  def parse(args: Array[String]): Config = {
    val conf = ConfigFactory.parseString(args.mkString("\n"))
    val size : Int = conf.getInt("size")
    val originPath = conf.getString("origin_path")
    val partitions = conf.getList("partitions")
    val filterArray = conf.getList("filter").unwrapped.toArray.map(_.asInstanceOf[util.ArrayList[String]])

    val filter = new Array[Array[String]](filterArray.size)
    var i = 0
    for (elem <- filterArray){
      filter(i) = elem.asScala.toArray
      i+=1
    }

    val config = Config(size, originPath, filter)

    config.filter.foreach(v => v.foreach(f => {if(!partitions.contains(f.split("=")(0))) throw new RuntimeException("The filter doesn't match the partitions")}))

    config
  }
}
