"""SimpleApp.py"""

import sys

from __future__ import print_function
from operator import add
from pyspark.sql import SparkSession

if __name__ == "__main__":

    logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
    spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
    logData = spark.read.text(logFile).cache()

    numAs = logData.filter(logData.value.contains('a')).count()
    numBs = logData.filter(logData.value.contains('b')).count()

    print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
    print("HELLJKLDJKLDFKLJKLFJD J>>>>>>>> ")

    spark.stop()