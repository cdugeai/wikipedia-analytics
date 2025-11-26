package com.example.detect


import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.util.Collector


import org.apache.flink.api.common.functions.AggregateFunction
import com.fasterxml.jackson.module.scala.deser.overrides
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.datastream.WindowedStream
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import com.example.model.WikipediaUpdate
import java.sql.Struct

// case class UserEditAcc(userCounts: Map[String, Int])
case class UserEdits(user_id: String, n_edits: Int)
case class SuspectPageAlert(url: String, qualified_users: Map[String, Int])


object DetectEditWar {

    val aggregate = new AggregateFunction[WikipediaUpdate, Map[String, Int], Map[String, Int]] {
        
        // Mapping (user -> n_edits)
        override def createAccumulator(): Map[String, Int] = Map.empty
        
        // Add +1 to the mapping (user -> n_edits)
        override def add(value: WikipediaUpdate, acc: Map[String, Int]): Map[String, Int] = {
            acc + (value.user -> (acc.getOrElse(value.user, 0) + 1))
        }
        
        override def getResult(acc: Map[String, Int]): Map[String, Int] = acc
        
        // Merge [(u1, 1), (u2, 3), (u1, 5)] => [(u1, 6), (u2, 3)]
        override def merge(a: Map[String, Int], b: Map[String, Int]): Map[String, Int] = {
            (a.keySet ++ b.keySet).map { key =>
                key -> (a.getOrElse(key, 0) + b.getOrElse(key, 0))
            }.toMap
        }
    }

    val process = new ProcessWindowFunction[Map[String, Int], SuspectPageAlert, String, TimeWindow] {
        override def process(
            pageId: String,
            context: Context,
            elements: Iterable[Map[String, Int]], // Always 1 element
            out: Collector[SuspectPageAlert]
        ): Unit = {

            val threshold_users = 2
            val threshold_edits = 2

            require(elements.size == 1, "Should only have one aggregate result")
            val userCounts = elements.head

            // DEBUG
            if (userCounts.size>1) {
                println("usercounts:"+userCounts)
            }

            // Users with >= 5 edits
            val qualifyingUsers = userCounts.filter(_._2 >= threshold_edits)
            // N users matching edits criterion
            if (qualifyingUsers.size >= threshold_users) {
                out.collect(SuspectPageAlert(pageId, qualifyingUsers))
            }
        }
    }

}
