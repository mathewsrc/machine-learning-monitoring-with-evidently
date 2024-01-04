from pyspark.sql import SparkSession
from prefect import task
from pyspark.ml.feature import VectorAssembler, Imputer, StandardScaler
from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator
)
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# MLflow (Tracking, Model registry, etc)
import mlflow
import mlflow.spark
from mlflow import MlflowClient
from matplotlib import pyplot as plt
from pathlib import Path
import numpy as np
from feature_engineering import build_feature_pipeline
import duckdb
from pyspark.sql.functions import col
from monitoring import eval_drift, eval_tests, build_column_mapping
import logging
import joblib
from utils.create_datasets import get_reference_period, get_experiments_batches
from tqdm import tqdm

spark = SparkSession.builder.appName("Supplier model training").getOrCreate()

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

def load_data(conn, table_name):
    return spark.createDataFrame(conn.sql(f"SELECT * FROM {table_name}").df())

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

    return image_path


def train_and_evaluate(model, model_name, experiment_batches,
                       column_mapping, label_col):
    try:
      logging.info("Training and Evaluating Model.")

      feature_pipeline = build_feature_pipeline(model, numeric_features, categorical_features)

      for k, test_dates in enumerate(experiment_batches):
        with duckdb.connect("supplier.db") as conn:
          reference_data = load_data(conn, "reference_table")
          current_data = load_data(conn, "current_table")

          print(f"\nBatch: {k}")
          print(f"Test interval: {test_dates}")

          pipeline_model = feature_pipeline.fit(reference_data)

          reference_data = pipeline_model.transform(reference_data)

          current_data = current_data.filter(
              (col("start_date") >= test_dates[0]) & (col("start_date") <= test_dates[1]))

          batch_size = current_data.count()
          print(f"\nBatch Size: {batch_size}")

          current_data = pipeline_model.transform(current_data)

          evaluator = BinaryClassificationEvaluator(labelCol=label_col,
                                                    rawPredictionCol="prediction",
                                                    metricName="areaUnderROC")

          columns_to_drop = ['rawPrediction', 'features',
                            'scaled_features', 'probability']

          reference_data = reference_data.drop(*columns_to_drop)

          current_data = current_data.drop(*columns_to_drop)

          # Make sure Target column is not empty
          if current_data.select(label_col).count() > 0:
              auc_reference = evaluator.evaluate(reference_data)
              print(f"{model_name} AUC (reference): {auc_reference}")
              mlflow.log_metric("AUC reference", auc_reference)

              auc_current = evaluator.evaluate(current_data)
              print(f"{model_name} AUC (current): {auc_current}")
              mlflow.log_metric("AUC current", auc_reference)

              mlflow.set_experiment_tag("model", model_name)

              metrics = eval_drift(reference=reference_data,
                                    production=current_data,
                                    column_mapping=column_mapping,
                                    model_name=model_name,
                                    tags=[model_name],
                                    batch_size=batch_size)

              tests = eval_tests(reference=reference_data,
                                    production=current_data,
                                    column_mapping=column_mapping,
                                    model_name=model_name,
                                    tags=[model_name],
                                    batch_size=batch_size)

              print("\nTests status")
              for feature in tests:
                  print(feature[0],':', feature[1])
                  mlflow.log_dict(feature[0], feature[1])

              print("\nMetrics score")
              for feature in metrics:
                  print(feature[0],':', round(feature[1], 3))
                  mlflow.log_metric(feature[0], round(feature[1], 3))

              registered_model_name = f"{model_name.lower()}_model_{test_dates[0]}_{test_dates[1]}"

              mlflow.spark.log_model(pipeline_model,
                                artifact_path="spark-model",
                                registered_model_name=registered_model_name)

              try:
                feature_importance_plot = feature_importance(pipeline_model,
                                                            numeric_features+categorical_features,
                                                             model_name)
              except Exception as e:
                logging.error(f"Error in feature_importance function: {e}")
                continue

              mlflow.log_artifact(feature_importance_plot)
    except Exception as e:
        logging.error(f"Error in train_and_evaluate function: {e}")
        raise e


def experiment():

    EXPERIMENT_NAME = "supplier_contract"
    mlflow.set_experiment(EXPERIMENT_NAME)

    column_mapping = build_column_mapping()
    label_col = column_mapping.target

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
            print(f"\n\nTraining and evaluating {model_name}")
            train_and_evaluate(model,
                                model_name,
                                get_experiments_batches(get_reference_period()[1]),
                                column_mapping, label_col)

