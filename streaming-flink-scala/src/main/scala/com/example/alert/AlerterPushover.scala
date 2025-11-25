package com.example.alert

import com.example.WindowResult

import sttp.client4.quick._


class AlerterPushover {
  def alertStdout(content: WindowResult) {
    print("Alert: "+ content.windowStart+s"${content.totalUpdates}>${content.distinctUsers}")
  }

  def alert(title: String, content: WindowResult) {

    val msg: String = s"${content.totalUpdates} modifications par ${content.distinctUsers} (${content.windowStart})"
    val pushover_token: Option[String] = sys.env.get("PUSHOVER_TOKEN")
    val pushover_user: Option[String] = sys.env.get("PUSHOVER_USER")
    
    pushover_token match {
      case None => throw new RuntimeException("No env var for PUSHOVER_TOKEN")
      case Some(value) => Unit
    }
    pushover_user match {
      case None => throw new RuntimeException("No env var for PUSHOVER_USER")
      case Some(value) => Unit
    }

    val json = ujson.Obj(
      "token" -> pushover_token.map(ujson.Str(_)).getOrElse(ujson.Null),
      "user" -> pushover_user.map(ujson.Str(_)).getOrElse(ujson.Null),
      "message" -> msg,
      "title" -> ("Alerte wiki " + title),
    )

    val response = quickRequest
        .post(uri"https://api.pushover.net/1/messages.json")
        .header("Content-Type", "application/json")
        .body(ujson.write(json))
        .send()

    println("ALERT "+title)
    //println(response.code)
    //println(response.body)
  }


}
