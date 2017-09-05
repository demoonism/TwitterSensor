"""
Inspired from komiya-atsushi's Scala code.
"""

from pyspark.ml.evaluation import Evaluator

import math
from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row
import numpy as np
from sklearn.metrics import average_precision_score
from pyspark.sql.functions import col

class BinaryRankingEvaluator(Evaluator):
    choice = ""
    def __init__(self, metric = "AP"):
        print("metrics is: ", metric)
        choice = metric
    def _evaluate(self, dataset, metric = "AP"):

        def precision(y_true, y_scores, k):
            act_set = set(y_true)
            pred_set = set(y_scores[:k])
            result = len(act_set & pred_set) / float(k)
            return result

        def recall(y_true, y_scores, k):
            act_set = set(y_true)
            pred_set = set(y_scores[:k])
            result = len(act_set & pred_set) / float(len(act_set))
            return result
        
        neg_slicer = VectorSlicer(inputCol="probability", outputCol="0_prob", indices=[0])
        pos_slicer = VectorSlicer(inputCol="probability", outputCol="1_prob", indices=[1])


        output_stg1 = neg_slicer.transform(dataset)
        output = pos_slicer.transform(output_stg1)

        Ranked_prediction  = output.sort(col("1_prob").desc())
        
        y_true = Ranked_prediction.select("label").rdd.flatMap(lambda x: x).collect()
        y_scores = Ranked_prediction.select("prediction").rdd.flatMap(lambda x: x).collect()

        score = 0
        if metric == "AP":
            score = average_precision_score(y_true, y_scores)  
        elif metric == "P100":
            score = precision(y_true,y_scores, 100)


        return score

    def isLargerBetter(self):
        return True