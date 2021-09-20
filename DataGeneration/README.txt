# Data Intensive Systems Project: Data Generation

DataGeneration
|
|_____ main_generate.py  : File to generate the dataset
|
|_____ entity.py : File that generates the expected dataset for one entity
|
|_____ generator.py : File that generates the whole dataset
|
|_____ entities_list.txt : List of entities considered
|
|____ README.txt
|


## Program Arguments
The program makes use of two arguments

Positional Argument | Description                                                        		   | Type    | 
:---                | :---                                                               		   | :---:   | 
`Entities list`     | The patht to where entities_list.txt is located	   				   | String  | 
`Data`              | The path where the dataset should be saved                                     	   | String  | 



To run the program, one can open a Terminal in the "DataGeneration" directory, then for example type:

python main_generate.py entities_list.txt <DESIRED SAVING PATH>

(Please note that both paths might depend on how you store the whole project)

