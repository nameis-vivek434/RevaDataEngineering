package utils

import java.io.FileInputStream
import java.util.Properties

object ConfigLoader {

  // Load application.properties
  val properties: Properties = {
    val prop = new Properties()
    val resourcePath = "src/main/resources/application.properties"
    prop.load(new FileInputStream(resourcePath))
    prop
  }

  // Optionally provide helper methods for retrieving values
  def get(key: String): String = properties.getProperty(key)
}