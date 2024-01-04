from evidently.spark.engine import SparkEngine
from evidently import ColumnMapping
from evidently.report import Report

from evidently.metric_preset import DataDriftPreset

from evidently.test_suite import TestSuite
from evidently.tests import (
    TestShareOfDriftedColumns,
    TestNumberOfDriftedColumns,
    TestNumberOfDuplicatedRows,
    TestHighlyCorrelatedColumns,
    TestNumberOfColumnsWithMissingValues,
    TestRocAuc,TestTPR,TestTNR,TestFPR,TestFNR)

import pendulum
import logging
from pathlib import Path

def build_column_mapping():
    
    column_mapping = ColumnMapping()

    column_mapping.prediction = "prediction"
    column_mapping.target = "is_non_profit"
    column_mapping.id = "contract_number"

    column_mapping.datetime_features = ['start_date',
                                        'end_date',
                                        'term_start_date',
                                        'term_end_date',]

    column_mapping.text_features = ['contract_title',
                                    'contract_type',
                                    'supplier_name',
                                    'supplier_name_prime_contractor',
                                    'department',
                                    'purchasing_authority',
                                    'scope_of_work',]

    column_mapping.categorical_features= ['project_team_lbe_status',
                                        'is_non_profit',
                                        'is_sole_source',
                                        'has_outstanding_orders',]

    column_mapping.numerical_features = ['contract_awarded_amount',
                                        'purchase_orders_outstanding',
                                        'payments_made',
                                        'remaining_contract_award_amount',]
    
    return column_mapping


def evaluate_drift(column_mapping, reference, production, model_name, tags=[], batch_size=None):
    try:
        reference_pandas = reference.toPandas()
        production_pandas = production.toPandas()

        logging.info("Running data drift analysis.")
        data_drift_report = Report(
            metrics=[
                DataDriftPreset(drift_share=0.4)
            ],
            batch_size=batch_size,
            tags=tags,
            metadata={
                "deployment": "test",
                "status": "production",
            },
            timestamp=pendulum.now()
        )

        data_drift_report.run(
            reference_data=reference_pandas,
            current_data=production_pandas,
            column_mapping=column_mapping,
            # engine=SparkEngine
        )
        
        if not Path('output/model/metrics').exists():
            Path('output/model/metrics').mkdir(parents=True)

        data_drift_report.save_html(
            f'output/model/metrics/{model_name.lower()}_data_drift_snapshot.html')
        data_drift_report.save(
           f'output/model/metrics/{model_name.lower()}_data_drift_snapshot.json')

        report = data_drift_report.as_dict()

        logging.info("Extracting feature drift scores.")

        drifts = []

        for feature in column_mapping.numerical_features + column_mapping.categorical_features:
            drifts.append((feature,
                           report["metrics"][1]["result"]["drift_by_columns"][feature]["drift_score"]))
        return drifts
    except Exception as e:
        logging.error(f"Error in eval_drift function: {e}")
        raise e
    
    
def load_parse_test_dict(tests):
  output = []
  for test in tests.as_dict()['tests']:
    output.append((test['name'], test['status']))
  return output


def eval_tests(reference, production,
               column_mapping, model_name,
               tags=[], batch_size=None):
  try:
    logging.info("Running tests analysis.")

    columns = ['contract_awarded_amount',
              'purchase_orders_outstanding',
              'payments_made',
              'remaining_contract_award_amount']

    tests = TestSuite(tests=[
                      TestNumberOfDriftedColumns(columns=columns, lt=0.4),
                      TestShareOfDriftedColumns(columns=columns, lt=0.4),
                      TestNumberOfDuplicatedRows(),
                      TestHighlyCorrelatedColumns(),
                      TestNumberOfColumnsWithMissingValues(gt=5, is_critical=False),
                      TestRocAuc(gt=0.8),
                      TestTPR(),
                      TestTNR(),
                      TestFPR(),
                      TestFNR()
                      ],
                      tags=tags,
                      timestamp=pendulum.now())

    tests.run(current_data=production.toPandas(),
              reference_data=reference.toPandas(),
              column_mapping=column_mapping,
              #engine=SparkEngine
              )

    
    if not Path('output/model/tests').exists():
        Path('output/model/tests').mkdir(parents=True)
        
    tests.save(
        f'output/model/tests/{model_name.lower()}_tests_snapshot.json')
    tests.save_html(
        f'output/model/tests/{model_name.lower()}_tests_snapshot.html')

    return load_parse_test_dict(tests)
  except Exception as e:
        logging.error(f"Error in eval_tests function: {e}")
        raise e