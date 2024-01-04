from pyspark.sql import SparkSession
from prefect import task
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator
)

# MLflow (Tracking, Model registry, etc)
import mlflow
import mlflow.spark
from mlflow import MlflowClient
from matplotlib import pyplot as plt
from pathlib import Path
import numpy as np
from feature_engineering import build_pipeline
import duckdb
from monitoring import  build_column_mapping
import logging
import joblib
from tqdm import tqdm
from datetime import timedelta
import os
from utils.global_variables import MLFLOW_TRACKING_URI, EXPERIMENT_NAME

spark = SparkSession.builder.appName("Supplier Contract").getOrCreate()


def get_features():
    numeric_features = ['contract_awarded_amount',
                    'purchase_orders_outstanding',
                    'payments_made',
                    'remaining_contract_award_amount'
                    ]

    categorical_features = ['project_team_lbe_status',
                            #'is_non_profit',
                            'has_outstanding_orders',
                            'is_sole_source'
                            ]
    return numeric_features, categorical_features

def setup_mlflow():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    experiment = mlflow.get_experiment_by_name(EXPERIMENT_NAME)
    
    return experiment.experiment_id

def load_data(table_name):
    with duckdb.connect("supplier.db") as conn:
            reference_data = spark.createDataFrame(conn.sql(f"SELECT * FROM {table_name}").df())
    return reference_data

def split_train_test(df, training_ratio=0.8, testing_ratio=0.2):
    training_data, testing_data = df.randomSplit([training_ratio, testing_ratio], seed=142)
    return training_data, testing_data

def feature_importance(pipeline, features, model_name):
    feature_importance = pipeline.stages[-1].featureImportances.toArray()

    fig, ax = plt.subplots()
    ind = np.arange(len(feature_importance))

    ax.bar(ind, feature_importance, align='center', color='skyblue')
    ax.set_xticks(ind)
    ax.set_xticklabels(features, rotation=45, ha="right")

    ax.set_ylabel('Feature Importance Score')
    ax.set_title('Feature Importance')

    plot_dir = Path("output/model/plots")
    if not plot_dir.exists():
        plot_dir.mkdir(parents=True)
        
    image_path = Path(plot_dir, f"{model_name}_feature_importance_plot.png")
    plt.savefig(image_path)
    mlflow.log_artifact(image_path)
    return image_path


@task(name="Run MFlow Experiment to train models",
      description="Runs experiment with PySpark ML models",
      tags=["experiment", "Spark ML", "training"],
      log_prints=True,
      cache_expiration=timedelta(days=1),
      retries=0,
      retry_delay_seconds=5)
def train():
    setup_mlflow()
    column_mapping = build_column_mapping()
    label_col = column_mapping.target
    
    reference_data = load_data("reference_table")
    training_data, testing_data = split_train_test(reference_data)
    
    numeric_features, categorical_features = get_features()
    
    evaluator = BinaryClassificationEvaluator(
                labelCol=label_col,
                rawPredictionCol="prediction",
                metricName="areaUnderROC")
    
    models = {
        "Random_Forest_Classifier": RandomForestClassifier(labelCol=label_col,
                                                            featuresCol='scaled_features'),
        "Logistic_Regression": LogisticRegression(labelCol=label_col,
                                                featuresCol='scaled_features'),
        "GBT_Classifier": GBTClassifier(labelCol=label_col,
                                        featuresCol='scaled_features')
    }

    for model_name, model in tqdm(models.items(), desc="Model Training"):
        with mlflow.start_run(run_name=model_name):
             # Enable MLflow autologging.
            mlflow.pyspark.ml.autolog(log_models=True, 
                                    log_datasets=False, 
                                    disable=False, 
                                    exclusive=False,
                                    disable_for_unsupported_versions=False, 
                                    silent=False, 
                                    log_post_training_metrics=True, 
                                    registered_model_name=True, 
                                    log_input_examples=True, 
                                    log_model_signatures=True, 
                                    log_model_allowlist=None, 
                                    extra_tags=None)
            pipeline = build_pipeline(model, numeric_features, categorical_features)

            pipeline_model = pipeline.fit(training_data)
            predictions = pipeline_model.transform(testing_data)
            
            auc = evaluator.evaluate(predictions)
            mlflow.log_metric("AUC", auc)
            
            mlflow.spark.log_model(pipeline_model,
                            artifact_path="spark-model",
                            registered_model_name=model_name.lower())


if __name__ == "__main__":
    train()