package org.chojin.spark.lineage

import java.util.Properties

import grizzled.slf4j.Logger

import scala.collection.JavaConversions._

object Config {
  private lazy val LOGGER = Logger[this.type]

  private final val prefix = "org.chojin.spark.lineage"
  private lazy val properties = {
    val stream = getClass.getResourceAsStream("/lineage.properties")
    val props = new Properties()
    props.load(stream)
    stream.close()

    props
  }

  def get(name: String): String = properties.getProperty(name)

  def getList(name: String): Seq[String] = Option.apply(properties.getProperty(name))
    .flatMap(p => if (p.isEmpty) None else Some(p))
    .map(p => p.split("\\s*,\\s*").toSeq)
    .getOrElse(Seq())

  def createInstanceOf[T](suffix: String): T = {
    val propPrefix = s"$prefix.$suffix"

    def clazz = getClass.getClassLoader.loadClass(get(propPrefix))
    val props = properties
      .toMap
      .filter({ case (k, _) => k.startsWith(s"$propPrefix.")})
      .map({ case (k, v) => k.substring(propPrefix.length + 1) -> v})

    LOGGER.info(s"Properties -> ${props.toMap}")

    clazz
      .getConstructor(classOf[Map[String, String]])
      .newInstance(props)
      .asInstanceOf[T]
  }

  def createInstancesOf[T](suffix: String): List[T] = {
    val propPrefix = s"$prefix.$suffix"

    getList(s"${propPrefix}s").map(className => {
      def clazz = getClass.getClassLoader.loadClass(className)
      val props = properties
        .toMap
        .filter({ case (k, _) => k.startsWith(s"$propPrefix.")})
        .map({ case (k, v) => k.substring(propPrefix.length + 1) -> v})

      LOGGER.info(s"Properties -> $props")

      clazz
        .getConstructor(classOf[Map[String, String]])
        .newInstance(props)
        .asInstanceOf[T]
    }).toList
  }
}
