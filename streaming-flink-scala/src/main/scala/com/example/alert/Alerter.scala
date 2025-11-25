package com.example.alert

import com.example.WindowResult

abstract class Alerter {
  def alert(title: String, msg: String): String
}
