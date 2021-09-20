import argparse
import program
from program import *
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser(description="Temporal similarity graph")
parser.add_argument("Threshold", type=float, help="The minimum threshold to be considered regarding the similarity measure")
parser.add_argument("data",type=str,help="The path where the data is located")
parser.add_argument("save",type=str,help="The path where the results should be saved")
args =parser.parse_args()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("INFOMDIS Project").master("local[*]").getOrCreate()
    sc = spark.sparkContext
    J = JaccardCalculator(spark,sc,args.Threshold,args.data,args.save)
    J.jaccard_comparison()
