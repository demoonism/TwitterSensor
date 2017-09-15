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
        validation = self.getOrDefault(self.valid)

        #metricName = eva.getMetricName()
        metricName = "AP"
        #print("metric is ", metricName)
              
        nFolds = self.getOrDefault(self.numFolds)
        seed = self.getOrDefault(self.seed)
        metrics = [0.0] * numModels

        print("train size ", dataset.count())
        print("valid size ",validation.count())

        for j in range(numModels):
            paramMap = epm[j]
            model = est.fit(dataset, paramMap)

            predictions = model.transform(validation, paramMap)
            #print(predictions.show())
            metric = eva.evaluate(predictions)
            metrics[j] += metric

            avgSoFar = metrics[j]

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
