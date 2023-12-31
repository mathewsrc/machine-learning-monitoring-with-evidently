# machine-learning-monitoring-with-evidently
ML Monitoring Capstone

## Summary
* Development of machine learning models with Pyspark ML.
* Model monitoring (ML monitoring) (data drift, prediction drift, and model performance metrics) with Evidentlyai.
* Extraction, Transformation, and loading of API data with Pyspark, Spark SQL, and DuckDB.
* Data quality check with the Soda library.
* Tracking and Model Registry with MLflow.

Sections

* [Problem Understading](#problem-understading)
* [Architecture Overview](#architecture-overview)
* [How to Run this Project](#how-to-run-this-project)
  * [Prerequisites](#prerequisites)
  * [Examples](#examples)
* [Project Structure](project-structure)
* [Data Collection](#data-collection)
* [Data Preparation](#data-preparation)
* [Model Training](#model-training)
* [Model Evaluation](#model-evaluation)
* [Model Deployment](#model-deployment)
* [Model Monitoring](#model-monitoring)


## Problem Understading

TODO

## Architecture Overview

TODO

## Project Structure

TODO

## How to Run this Project


### Generating plots using CLI

Run the following command to see all CLI options

```bash
poetry run python src/visualizations/visualizer.py 
```

For example we can run the following command to visualize the Contracts over time

```bash
poetry run python src/visualizations/visualizer.py contracts-over-time
```

### Prerequisites

TODO

### Examples

TODO

## Data Collection

TODO

## Data Preparation

TODO

![image](https://github.com/mathewsrc/machine-learning-monitoring-with-evidently/assets/94936606/35957339-272b-44e9-bac0-5e78d0e7d300)


## Model Training

TODO

![image](https://github.com/mathewsrc/machine-learning-monitoring-with-evidently/assets/94936606/0e560813-13aa-424c-ac78-e6338ec13166)


## Model Evaluation

Batches date period

```
┌─────────────────┬────────────┐
│ min(start_date) │  end_date  │
│      date       │    date    │
├─────────────────┼────────────┤
│ 2022-01-03      │ 2022-07-03 │
│ 2022-02-01      │ 2022-08-01 │
│ 2022-03-01      │ 2022-09-01 │
└─────────────────┴────────────┘
```

Reference dataset

```
1991-12-03 00:00:00
2021-01-01 00:00:00
```



![image](https://github.com/mathewsrc/machine-learning-monitoring-with-evidently/assets/94936606/bd8e8b6f-f030-44d7-af12-e835813d5e29)


## Model Deployment

TODO

## Model Monitoring

TODO

TestSuit result (partially)

![image](https://github.com/mathewsrc/machine-learning-monitoring-with-evidently/assets/94936606/b755b242-bacc-4c4f-b60e-935d444e1cc2)

Metrics result (partially)

![image](https://github.com/mathewsrc/machine-learning-monitoring-with-evidently/assets/94936606/ebc5ab66-c4b9-420e-b3f6-46e428a70f55)

![image](https://github.com/mathewsrc/machine-learning-monitoring-with-evidently/assets/94936606/b9f0e1a2-fa96-4d46-aedf-7f2bbe8e211d)







