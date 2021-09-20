import requests
import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup as bs
import datetime
from collections import Counter

class Entity:

    def __init__(self,name):
        self.name = name

    # Retrieves versions of the entity according to some criteria
    def get_revisions(self):

        """

        Returns: 
                List of tuples, [(date, version IDs),..]
        
        """
        # Retrieves HTML from "View History" page of the entity
        # time= 10000, all pages can show up to 5000 versions
        entity_history = requests.get(f"https://en.wikipedia.org/w/index.php?title={self.name}&offset=&limit=10000&action=history")
        
        # Initiliazes parser 
        soup = bs(entity_history.text,"lxml").find_all("li")
        
        history = []
        trace = Counter([2021-i for i in range(22)]) 

        for part in soup:
            # date 
            date = re.search(r"[0-9]{1,2}\:[0-9]{1,2}\, [0-9]{1,2} [A-Z][a-z]* [0-9]{4}", part.prettify())
            
            # page id
            version_id = part.get("data-mw-revid")
            if (date != None) and (version_id != None):

                if (len(soup) < 1000) and trace[int(date.group()[-4:])] == 1:
                    history.append((date.group(),version_id))
                    trace[int(date.group()[-4:])] += 1

                elif (1000 < len(soup) < 5000) and trace[int(date.group()[-4:])] < 3:
                    history.append((date.group(),version_id))
                    trace[int(date.group()[-4:])] += 1
                    
                elif (len(soup) > 5000) and trace[int(date.group()[-4:])] < 6:
                    history.append((date.group(),version_id))
                    trace[int(date.group()[-4:])] += 1

        return history

    def get_related_entities(self):
       
        """

        Returns: 
                pandas.DataFrame, storing all of the references to other entities for each version

        """

        entity_versions = self.get_revisions() 
        i = 0
        try:
            for version_id in entity_versions:

                if i%3 == 0:
                    print(f"Percentage accomplished {(i/len(entity_versions))*100} %")
                    print()
                res = requests.get(f"https://en.wikipedia.org/w/index.php?title={self.name}&oldid={version_id[1]}")
                soup = bs(res.text, "lxml")
                related_entities = np.array([])

                for link in soup.find_all("a"):

                    if link.text.strip() == "Article":
                        break
                    url = link.get("href", "")
                    
                    # Conditions for removing useless links
                    if ("/wiki/" in url) and (":" not in url) and ("identifier" not in url):
                        related = re.sub("/wiki/","",url)
                        related = re.sub(",","",related) # Avoids causing a mess when creating RDDs
                        related_entities = np.append(related_entities, related)
                
                related_entities = np.unique(related_entities) # Avoids causing repetition in related entities
                try:
                    related_entities= np.insert(related_entities,0,re.sub(",",'',version_id[0]))
                    
                    # related_entities= np.insert(related_entities,0,version_id[1])
                    related_entities = pd.Series(related_entities)
                    
                    if i == 0:
                        entity_data = pd.DataFrame(related_entities)
                        
                    else:
                        entity_data = pd.concat([entity_data,related_entities],axis = 1, ignore_index=False)

                except: # some versions are not available (the ones which are barred)
                    continue
                i += 1
            return entity_data.T # Transpose to get the desired structure
        except:
            pass