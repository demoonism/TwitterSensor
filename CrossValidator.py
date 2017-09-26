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


import numpy as np

from pyspark.ml.tuning import CrossValidator, CrossValidatorModel, ParamGridBuilder
from pyspark.sql.functions import rand
from LogLossEvaluator import BinaryRankingEvaluator


class CrossValidatorVerbose(CrossValidator):

    def _fit(self, dataset):
        result = []

        est = self.getOrDefault(self.estimator)
        epm = self.getOrDefault(self.estimatorParamMaps)
        numModels = len(epm)
        print("total of", numModels, " models")
              
        eva = self.getOrDefault(self.evaluator)

        #metricName = eva.getMetricName()
        metricName = "AP"
        #print("metric is ", metricName)
        
        nFolds = self.getOrDefault(self.numFolds)
        seed = self.getOrDefault(self.seed)
        h = 1.0 / nFolds
        metrics = [0.0] * numModels
        
        train = dataset.where(col("type") == "train").persist()
        valid = dataset.where(col("type") == "valid").persist()
        
        randCol = self.uid + "_rand"
        df = train.select("*", rand(seed).alias(randCol))

        for i in range(nFolds):
            foldNum = i + 1
            print("Comparing models on fold %d" % foldNum)

            LeaveOffLB = i * h
            LeaveOffUB = (i + 1) * h
            condition = (df[randCol] >= LeaveOffLB) & (df[randCol] < LeaveOffUB)
            train = df.filter(~condition)
            print("train size ", train.count())
            print("valid size ",valid.count())
                        
            for j in range(numModels):
                paramMap = epm[j]
                model = est.fit(train, paramMap)

                predictions = model.transform(valid, paramMap)
                #print(predictions.show())
                metric = eva.evaluate(predictions)
                metrics[j] += metric

                avgSoFar = metrics[j] / foldNum

                res=("params: %s\t%s: %f\tavg: %f" % (
                    {param.name: val for (param, val) in paramMap.items()},
                    metricName, metric, avgSoFar))
                result.append(res)
                print(res)

        if eva.isLargerBetter():
            bestIndex = np.argmax(metrics)
        else:
            bestIndex = np.argmin(metrics)

        bestParams = epm[bestIndex]
        bestModel = est.fit(dataset, bestParams)
        avgMetrics = [m / nFolds for m in metrics]
        bestAvg = avgMetrics[bestIndex]
        print("Best model:\nparams: %s\t%s: %f" % (
            {param.name: val for (param, val) in bestParams.items()},
            metricName, bestAvg))

        return self._copyValues(CrossValidatorModel(bestModel, avgMetrics))
