package com.example.detect

import com.example.model.WikipediaUpdate
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.KeyedStream
import org.apache.flink.streaming.api.scala.WindowedStream
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

abstract class Detector {
  def detect(stream: WindowedStream[WikipediaUpdate,String,TimeWindow]): DataStream[Any] 
}

/* 

## How works Aggregate and Process windowed results

Window with 100 events
    ↓
AggregateFunction processes them incrementally
    ↓ (add called 100 times, building up the accumulator)
getResult() produces ONE Map[String, Int]
    ↓
ProcessWindowFunction receives Iterable with that ONE map
    ↓
elements.head gets that single map 

*/