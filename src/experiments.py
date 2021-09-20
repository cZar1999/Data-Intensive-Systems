import argparse
import program
from program import *
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import random
import os
import shutil
from datetime import datetime
import json


parser = argparse.ArgumentParser(description="Experiments")
parser.add_argument(
    "save", type=str, help="The path where the results should be saved")
args = parser.parse_args()


# ent = [i+50 for i in range(0, 500, 50)]
ent = [155]

DIR = '/home/cesar/Desktop/INFOMDIS/Project/Experiments'
THRESHOLD = 0.000005
times_dictionnary = {}


def get_directory_size(directory):
    """Returns the `directory` size in bytes."""
    total = 0
    try:
        # print("[+] Getting the size of", directory)
        for entry in os.scandir(directory):
            if entry.is_file():
                # if it's a file, use stat() function
                total += entry.stat().st_size
            elif entry.is_dir():
                # if it's a directory, recursively call this function
                total += get_directory_size(entry.path)
    except NotADirectoryError:
        # if `directory` isn't a directory, get the file size then
        return os.path.getsize(directory)
    except PermissionError:
        # if for whatever reason we can't open the folder, return 0
        return 0
    return total


if __name__ == "__main__":

    spark = SparkSession.builder.appName("Experiments").master(
        "local[*]").getOrCreate() # .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    sc = spark.sparkContext
    print(f"----Test will be performed with threshold {THRESHOLD} and {ent} as the number of entities------")

    # First, loop over the version numbers
    whole_data = os.listdir(DIR + "/" + "dataEXP")
    whole_data_path = DIR + "/" + "dataEXP"
    fails = []
    for number_of_entities in ent:

        # Create directory for random number of entities to be copied
        current_folder = DIR + "/" + f"data{number_of_entities}"
        os.mkdir(current_folder)

        # Select random files to be copied
        files = random.sample(whole_data, number_of_entities)
        for file in files:
            shutil.copy(whole_data_path + "/" + file,
                        current_folder + "/" + file)

        size = get_directory_size(current_folder)
        # Now that we have the folder, the test can be ran from the current_folder
        start = datetime.now()
        J = JaccardCalculator(spark, sc, THRESHOLD,
                              current_folder, args.save, saving=False)
        simil = J.jaccard_comparison()
        times_dictionnary[f"{number_of_entities}_time"] = [str(datetime.now() - start), size]
        print("HEREEEEE")
        print(simil)
        try:
            print("BENCHMARK")
            fileJ = json.dumps(times_dictionnary)
            jsonF = open("times2.json","w")
            jsonF.write(fileJ)
            jsonF.close()
            """
            similJ = json.dumps(simil)
            similF = open("similarities.json","w")
            similF.write(similJ)
            similF.close()
            """
            print(times_dictionnary[f"{number_of_entities}_time"])
        
        except:
            continue

        # times_dictionnary[f"{number_of_entities}_time"] = "Needs re run"

    print(times_dictionnary)
    a_file = open("times.json", "w")
    json.dump(times_dictionnary, a_file)
