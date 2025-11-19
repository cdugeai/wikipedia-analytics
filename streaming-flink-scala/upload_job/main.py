import requests
from dotenv import dotenv_values

config: dict = {
    **dotenv_values(".env.shared"),
    **dotenv_values(".env"),
}


def upload() -> str:
    """
    Uploads JAR and return its id (jarid)
    """
    url = f"http://{config.get('FLINK_HOST')}:{config.get('FLINK_PORT')}//jars/upload"

    response = requests.post(
        url,
        files={
            "jarfile": open(config.get("JAR_FILEPATH"), "rb"),
        },
    )

    filename = response.json().get("filename")
    jarid = filename.split("/flink-web-upload/")[1]

    print("JAR uploaded:", jarid)
    return jarid


def trigger_run(jarid: str) -> str:
    """
    Triggers the run of a specific jar (with its jarid)
    """
    response = requests.post(
        f"http://{config.get('FLINK_HOST')}:{config.get('FLINK_PORT')}//jars/{jarid}/run"
    )
    jobid = response.json().get("jobid")

    print("Job triggered:", jobid)
    return jobid


if __name__ == "__main__":
    jarid = upload()
    job_id = trigger_run(jarid)
