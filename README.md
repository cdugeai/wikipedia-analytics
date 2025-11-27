# Wikipedia analytics :bar_chart:

This project is a benchmark of big data tools to handle large volumes of data.

## _How long would it take to read the entire Wikipedia?_

_About 30 seconds with the right tools!_

In this project, I was able to calculate some interesting statistics about the content of the French Wikipedia: 22 billion characters, 2.7 million articles, 43,000 films, and many more. [Link to the app](https://wikipedia-analytics.vercel.app/) :dart:

:arrow_right: See folder [python-polars](./python-polars/) and [python-pyspark](./python-pyspark/).

_Stack: Pyspark, Polars, SQL, Python_

## Real-time alerts :mega:

One part of the project is about detecting anomalies in Wikipedia updates **in real-time**. Here is the overview of the project.

|        Project overview        |
| :----------------------------: |
| ![](docs/overview_project.png) |

The alerts are sent directly to a phone in real-time when an anomaly is detected:

|                         Alerts on multiple edits                          |                     Alerts on _Edit War_                     |
| :-----------------------------------------------------------------------: | :----------------------------------------------------------: |
| ![](streaming-flink-scala/docs/mockups-notif/two_notifs-portrait-min.png) | ![](streaming-flink-scala/docs/mockups-notif/edit-war-1.png) |

:arrow_right: See folder [streaming-flink-scala](./streaming-flink-scala/)

_Stack: Flink, Kafka, Python_
