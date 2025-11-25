package com.example.alert

import com.example.WindowResult

abstract class Alerter {
  def alert(title: String, content: WindowResult): Unit
}
