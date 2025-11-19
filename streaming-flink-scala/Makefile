update:
	sbt update
build:
	sbt compile && sbt assembly
upload:
# Upload JAR to Flink cluster and run it 
	cd upload_job && uv run python3 main.py