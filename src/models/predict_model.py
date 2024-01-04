from prefect import task, Flow
import mlflow
from mlflow import MlflowClient
import os
from utils.global_variables import MLFLOW_TRACKING_URI, EXPERIMENT_NAME

def get_best_model():
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    client = MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    run = MlflowClient().search_runs(experiment_ids=experiment.experiment_id, order_by=["metrics.accuracy DESC"])[0]
    run_id = run.info.run_id
    model_uri = f"runs:/{run_id}/mlruns"

    filter_string = "run_id='{}'".format(run_id)
    results = client.search_model_versions(filter_string)
    model_version=results[0].version

    return model_version, model_uri 


if __name__ == "__main__":
    model_version, model_uri = get_best_model()
    print(model_version)
    print(model_uri)