from entity import Entity

class DatasetGenerator:

    def __init__(self, entities_path, save_path):

        self.entities_path = entities_path # Path to entities_list.txt
        self.save_path = save_path

        self.entities_list = []
        with open(entities_path,"r") as entities_list:
            entities_list = entities_list.read().splitlines()

        # Changes entities' format to gain time in HTML booting
        for i in entities_list:
            self.entities_list.append(i.replace(" ","_")) 

    # Generate the dataset in the desired structure
    def generate(self):
        j = 0

        for i in self.entities_list:

            if j % 6 == 0: # Callback frequency
                print()
                print(f"Entities Processed ... {(j/len(self.entities_list)*100)} %")
                print()
                print()

            ent = Entity(i)
            print(f"Currently processing {ent.name}")
            ent = ent.get_related_entities()
            try:
                if len(ent.columns) > 3:
                    ent.to_csv(self.save_path+i, header = False,index = False)
            except:
                pass
                
                j += 1
        print("JOB COMPLETED")
        return