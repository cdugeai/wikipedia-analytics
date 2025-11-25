package com.example.model

import java.time.{Instant, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

case class WikipediaUpdate(wiki: String, timestamp: Long, user: String){

  def sinkOutput: String = s"$wiki, $timestamp, $user"

  override def toString: String = 
    s"WikipediaUpdate($wiki, ${timestamp}, $user)"
  

}

object WikipediaUpdate {
  val error = WikipediaUpdate("error reading", 0L, "no_user")

  def fromString(string: String): WikipediaUpdate =
    Try {
      val Array(wiki, timestamp, user) = string.split(',')
      WikipediaUpdate(
        wiki.trim,
        timestamp.trim.toLong,
        user.trim
      )
    }.getOrElse(error)
}

