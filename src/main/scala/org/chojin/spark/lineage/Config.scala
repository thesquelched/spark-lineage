package org.chojin.spark.lineage

import java.util.Properties
import scala.collection.JavaConversions._

import org.chojin.spark.lineage.reporter.Reporter

object Config {
  private final val prefix = "org.chojin.spark.lineage"
  private lazy val properties = {
    val stream = getClass.getResourceAsStream("/lineage.properties")
    val props = new Properties()
    props.load(stream)
    stream.close()

    props
  }

  def get(name: String): String = properties.getProperty(name)

  def createInstanceOf[T](suffix: String): T = {
    def clazz = getClass.getClassLoader.loadClass(get(s"$prefix.$suffix"))
    val props = properties.toMap.filter({ case (k, _) => k.startsWith(s"$prefix.$suffix.")})

    clazz
      .getConstructor(classOf[Map[String, String]])
      .newInstance(props)
      .asInstanceOf[T]
  }
}
