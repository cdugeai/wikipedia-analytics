package com.example.model

import java.time.{Instant, ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

case class WikipediaUpdateMeta(id: String, uri: String) extends Serializable

case class WikipediaUpdate(
  wiki: String, 
  timestamp: Long, 
  user: String, 
  meta: WikipediaUpdateMeta // or @JsonProperty("meta.id") metaId: String
) extends  Serializable{

  def sinkOutput: String = s"$wiki, $timestamp, $user"

  override def toString: String = 
    s"WikipediaUpdate($wiki, ${timestamp}, $user)"
  

}

object WikipediaUpdate {
  val error = WikipediaUpdate("error reading", 0L, "no_user", WikipediaUpdateMeta("no_id", "no_uri"))

  def fromString(string: String): WikipediaUpdate =
    Try {
      val Array(wiki, timestamp, user, meta_id, meta_uri) = string.split(',')
      WikipediaUpdate(
        wiki.trim,
        timestamp.trim.toLong,
        user.trim,
        WikipediaUpdateMeta(meta_id, meta_uri)
      )
    }.getOrElse(error)
}

