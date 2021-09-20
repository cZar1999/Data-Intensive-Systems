import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
import os
import re
import datetime
import itertools
import csv # used to return node list 
import time



class JaccardCalculator:

    def __init__(self,spark,sc,threshold, data_path, save_path, saving = True):

        """
        threshold: minimum threshold for Jaccard Similarity
        path: path to the located entities
        """
        self.spark = spark
        self.sc = sc
        self.threshold = threshold
        self.data_path = data_path
        self.save_path = save_path
        self.saving = saving

    
    # Computes all of the possible pairs that can be made out of the entities
    # Returns a list of cardinality: BinomialCoefficient( #(number of entities) , 2)
    def compute_pairs(self):
        return list(itertools.combinations(os.listdir(path=self.data_path),r=2))
        

    # Utility function, Puts date into right format for comparison
    @staticmethod 
    def conv_date_list(list_related):
        date = list_related[0]

        date = datetime.datetime(year = int(re.search(r"[0-9]{4}",date).group()),\
                                    month = datetime.datetime.strptime(re.search(r"[A-Z][a-z]+",date).group(),"%B").month,\
                                    day = int(re.search(r" [0-9]{1,2} ",date).group()),\
                                    hour = int(re.search(r"[0-9]{2}",date).group()),\
                                    minute =int(re.search(r":[0-9]{2}",date).group()[1:]))
        
        list_related[0] = date
        list_related = list(filter(lambda x: x != "",list_related))

        return list_related


    # Loads an entity into and RDD
    def load_entity(self,entity_path):
        entity = self.sc.textFile(entity_path)\
                .map(lambda line: line.split(","))\
                .map(JaccardCalculator.conv_date_list)
        return entity 
    
    

    # Gets time intervals for an entity
    def get_intervals(self,entity):
        # Retrieves dates
        intervals = entity.flatMap(lambda x: x).filter(lambda x: type(x) == datetime.datetime)
        intervals = intervals.zipWithUniqueId()
        org = intervals.map(lambda x: (x[1], x[0]))
        shifted = intervals.map(lambda x: (x[1]-1, x[0]))
        intervals = org.join(shifted).values()
        
        return intervals
    
    # Get all the intervals that overlap
    # Computes Aleph and Aleph'
    @staticmethod
    def overlaps(dates):

        if (dates[0][1] > dates[1][0]) and (dates[0][0] > dates[1][1]):

            return False

        elif (dates[1][0] > dates[0][1]) and (dates[1][1] > dates[0][0]):

            return False

        else:
            
            return True

    # [(dateA,dateB),..] returns all legal dates 
    # Computes Theta(A,B)
    def dates_to_compare(self,intervals_A,intervals_B):

        overlapping = intervals_A.cartesian(intervals_B)
        compare = overlapping.filter(JaccardCalculator.overlaps).map(lambda x: (x[0][1],x[1][1]))

        return compare

    # Jaccard similarity
    # mu(A,B)
    @staticmethod
    def jaccard(A,B):
        if len(set(A).intersection(B)) > 1:
            jaccard = float(len(set(A).intersection(B)))/float(len(set(A).union(B)))
            return jaccard
        else:
            return None
    
    # Does the computations for pair of entities
    # Returns Mu(A,B)
    def pair_data(self,path_entityA,path_entityB):
        
        entity_A = self.load_entity(path_entityA)
        entity_B = self.load_entity(path_entityB)

        intervals_A = self.get_intervals(entity_A)
        intervals_B = self.get_intervals(entity_B)

        comparisons = self.dates_to_compare(intervals_A, intervals_B)

        dates_A = set(comparisons.map(lambda x: x[0]).collect())
        dates_B = set(comparisons.map(lambda x: x[1]).collect())

        # Retains date that need comparison, then removes dates as we do not want them for Jaccard comparison
        # Computes Theta(A,B)
        versions_A = entity_A.filter(lambda x: x[0] in dates_A).map(lambda x: x[1:])
        versions_B = entity_B.filter(lambda x: x[0] in dates_B).map(lambda x: x[1:])
        

        data_A = self.spark.createDataFrame(
                versions_A.zipWithUniqueId(),
                StructType([
                    StructField("Entity_A", ArrayType(StringType())),
                    StructField("VersionID", LongType())
                ])
            )
        
        data_B = self.spark.createDataFrame(
                versions_B.zipWithUniqueId(),
                StructType([
                    StructField("Entity_B", ArrayType(StringType())),
                    StructField("VersionID", LongType())
                ])
            )
        
        df = data_A.join(data_B,"VersionID").select("Entity_A","Entity_B")
        
        weighted_jacc = f.udf(JaccardCalculator.jaccard)
        JAC = weighted_jacc(df["Entity_A"], df["Entity_B"])
        df = df.withColumn("Jaccard", JAC)
        mean = df.agg({"Jaccard": "avg"}).collect()[0]["avg(Jaccard)"]
        
        if mean != None and mean > 0:
            return mean
        else:
            return None

    # Yields the final results
    def jaccard_comparison(self):

        schema = StructType([
            StructField('Entity_A',dataType= StringType()),
            StructField('Entity_B',dataType= StringType()),
            StructField('Jaccard_Similarity', dataType= FloatType())
        ])
        
        DF = self.spark.createDataFrame([], schema)
        
        pairs = self.compute_pairs()
        # i = 0
        p =0
        print(f"There are {len(pairs)} to compute")
        sim = {}
        counter = 0
        start = time.time()
        for entity_A, entity_B in pairs:
            try: 
                jaccard = self.pair_data(path_entityA= self.data_path+"/"+entity_A, path_entityB=self.data_path+"/"+entity_B)
                # counter += 1
                if p % 6 == 0:
                    
                    print("A:{}".format(entity_A))
                    print("B:{}".format(entity_B))
                    # print(type(jaccard))
                    print("Percentage of current data treated:")
                    print(p/len(pairs))                
                p += 1
                
                # Only entities above the similarity are kept in the final dataframe
                if jaccard !=None and jaccard >= self.threshold:

                    newRow = self.spark.createDataFrame([(entity_A,entity_B,jaccard)],schema=schema)
                    DF = DF.union(newRow)
                    # print(f"{(i/len(pairs))*100} % of distinct pairs are above threshold {self.threshold}")
                    print("SIMILAR BY:")
                    print(f"amount {jaccard}")
                    sim[frozenset({entity_A,entity_B})] = jaccard
                    # i += 1,100
            except:
                continue
        # Saves data
        if self.saving == True:
            DF.repartition(1).write.options(header=True).csv(self.save_path) # This was on before +"/"+"Result"
        else:
            print("Comparisons done and not saved")
        
        """
        # Node description
        entities =  DF.select('Entity_A','Entity_B').distinct().toPandas()
        entities_list =  list(entities["Entity_A"])
        entities_list.extend(list(entities["Entity_B"]))
        entities_list = list(set(entities_list)) # Removes duplicates, must be done sequentially
        entities_list = [[i] for i in entities_list]
        entities_list.insert(0,["Entity"]) # "Entities" as the header
        file = open(self.save_path + "/" + 'Nodes.csv', 'w+', newline ='')

        with file:    
            write = csv.writer(file)
            write.writerows(entities_list)
        """
        return sim
