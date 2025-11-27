package com.example.alert

abstract class Alerter {
  def alert(title: String, msg: String): String
}
