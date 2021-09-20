import argparse
from generator import DatasetGenerator

#data/ for effective saving
parser = argparse.ArgumentParser(description="Generate a dataset of wikipedia pages as entities")
parser.add_argument("entities", type=str,metavar="", help="The path to the file entities_list.txt")
parser.add_argument("save",type=str, metavar="",help="The path where the data should be saved")
args =parser.parse_args()

if __name__ == "__main__":
    gen = DatasetGenerator(args.entities, args.save)
    gen.generate()