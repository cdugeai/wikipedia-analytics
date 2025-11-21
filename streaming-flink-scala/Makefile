local.run:
	sbt clean run
cluster.build:
	sbt clean assembly
cluster.submit:
	cp target/scala-2.12/streaming-flink-scala-assembly-0.1.0.jar jars/submitted.jar
	docker exec -it flink_jobmanager /opt/flink/bin/flink run /opt/flink/usrlib/submitted.jar
cluster.logs:
# Print job logs
	docker container logs --follow flink_taskmanager
