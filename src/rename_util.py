import os
import re
# print(os.listdir("/home/cesar/Desktop/INFOMDIS/Project/Experiments/dataEXP"))
SRC = "/home/cesar/Desktop/INFOMDIS/Project/Experiments/dataEXP"
for i in os.listdir(SRC):
    print(i)
    if ',' in i:
        print("CHAHAHAHAAH")
        print(i)
        os.rename(SRC + "/" + i, SRC + "/" + re.sub(",","",i))