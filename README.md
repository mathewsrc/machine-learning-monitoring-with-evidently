# machine-learning-monitoring-with-evidently
ML Monitoring Capstone

## Summary
* Desenvolvimento de modelos de machine learning com Pyspark ML.
* Monitoramento de modelos (ML monitoring)  (data drift, prediction drift e  métricas de performance dos modelos) com Evidentlyai.
* Extração, Transformação e carregamento de dados de API com Pyspark, Spark SQL e DuckDB.
* Checagem de qualidade de dados coma biblioteca Soda.
* Tracking e Model Registry com MLflow.
* Através do projeto foi possível desenvolver um modelo com AUC igual a 75% e monitorar data drift nos dados e prediction drift no modelo.

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







