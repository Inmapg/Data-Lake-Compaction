package dev

import scopt.OptionParser

case class Arguments(configFileName: String = "")

/**
 * Arguments parser
 * @author Inmapg
 */
object Arguments {

  val parser = new OptionParser[Arguments]("data-lake-compaction") {
    head("data-lake-compaction", "0.0.0.1")

    opt[String]('p', "configFile")
      .required()
      .action((cp, a) => a.copy(configFileName = cp))
      .text("Path where the configuration file is located")
  }

  def parse(args: Seq[String]): Arguments =
    parser
      .parse(args, Arguments())
      .getOrElse(throw new RuntimeException("Error during arguments parsing"))
}
