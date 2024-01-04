from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, Imputer, StandardScaler, StringIndexer, OneHotEncoder
)

def build_pipeline(model, numeric_cols, categorical_cols):
    indexers = [StringIndexer(inputCol=col, outputCol=f"{col}_indexed")
    for col in categorical_cols]

    encoders = [OneHotEncoder(inputCol=f"{col}_indexed",
                              outputCol=f"{col}_encoded")
    for col in categorical_cols]

    assembler = VectorAssembler(inputCols=numeric_cols +
     [f"{col}_encoded" for col in categorical_cols], outputCol="features")

    imputer_numeric = Imputer(strategy="mean",
                              inputCols=numeric_cols,
                              outputCols=[f"{col}_imputed"
                                          for col in numeric_cols])

    imputer_categorical = Imputer(strategy="most_frequent",
                                  inputCols=[f"{col}_encoded"
                                             for col in categorical_cols],
                                  outputCols=[f"{col}_imputed"
                                              for col in categorical_cols])

    scaler = StandardScaler(inputCol="features",
                            outputCol="scaled_features")

    return Pipeline(stages=indexers + encoders + [assembler, imputer_numeric, scaler, model])