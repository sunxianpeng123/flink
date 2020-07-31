package com.xiaohulu.tools

import java.io.File
import java.util.concurrent.ConcurrentHashMap

import scala.io.Source

object PropertyUtil {

  private val props = new ConcurrentHashMap[String, String]()

  def load(filePath: String): Unit = {
    val file = new File(filePath)
    if (!file.exists()) {
      throw new IllegalArgumentException(s"file '${filePath}' not found")
    }
    Source.fromFile(file).getLines.filter(!_.startsWith("#")).filter(_.trim.length!=0)
      .map(_.split("=")).foreach(line => set(line(0).trim, line(1).trim))
  }


  def load(uri: java.net.URL): Unit = {
    scala.io.Source.fromURL(uri).getLines().filter(!_.startsWith("#")).filter(_.trim.length!=0)
      .map(_.split("=")).foreach(line => set(line(0).trim, line(1).trim))
  }

  private[this] def set(key: String, value: String): Unit = {
    if (key == null) {
      throw new NullPointerException("key is null")
    }
    props.put(key, value)
  }

  def getOption(key: String): Option[String] = {
    Option(props.get(key))
  }

  def getString(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  def getString(key: String): String = {
    getOption(key).get
  }

  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }
}