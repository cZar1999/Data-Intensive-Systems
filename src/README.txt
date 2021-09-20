# Data Intensive Systems Project: Spark Solution

src
|
|_____ main_sim.py
|
|_____ program.py
|
|____ README.txt
|


## Program Arguments
The program makes use of three arguments

Positional Argument | Description                                                        		   | Type    | 
:---                | :---                                                               		   | :---:   | 
`Threshold`         | The threshold chosen for entities to be similar w.r.t the Jaccard similarity 	   | Float   | 
`Data`              | The path to load the entities from                                     	           | String  | 
`Save`              | The path to load the resulting files                           		  	   | String  | 


To run the program, one can open a Terminal in the "src" directory, then for example type:

python main_sim.py 0.015 Project/data Project/graph

(Please note that both paths might depend on how you store the whole project)

PS:  
Console logs were not removed,  "WARN: Block input-* already exists on this machine; not re-adding it" is displayed,
this is normal when running in local mode.
(source: http://apache-spark-user-list.1001560.n3.nabble.com/quot-Block-input-already-exists-on-this-machine-not-re-adding-it-quot-warnings-td12646.html)
