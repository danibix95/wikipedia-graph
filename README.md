# Wikipedia-graph

This is the project for the Big Data and Social Network course at University of Trento for the a.y. 2017/2018.

## Project requirements
In order to run all the modules of the project, the following programs have to be installed:

+ Scala programming language (v2.11.*)
+ SBT (Simple Build Tool)
+ Java JDK (v8)
+ Spark libraries (v2.3.* - http://spark.apache.org/)
+ Neo4j (v3.4.5 - https://neo4j.com/)
+ NodeJS (v10.3.0 or later)

## Run the processing pipeline
Before running the processing pipeline, a set of folders must be created, where files are going to be held (HDFS or local filesystem), according to this structure:

```
main-resource-folder
 |-- input
 |     |-- input1
 |     |    |-- file1.xml
 |     |    |-- file2.xml
 |     |    |-- file3.xml
 |     |-- input2
 |-- W2V
```

Then the Word2Vec model has to be translated (if it has not been done before) from the plaintext to parquet version.
To do so, it is possible to run the script inside the folder `convertW2V`. So, from the terminal run the following commands (for running locally)

    cd parent-folder/convertW2V
    JAVA_OPT="-Xmx8G" sbt "run <path_main-resource-folder> <path_W2Vplaintext>"

Depending on the amount of available memory and size of the selected model, `-Xmx` param can be adjusted.

Now it is possible to run the processing pipeline. This can be done locally or on a cluster in the following manners:

+ Locally
        
      cd <path_wikipedia-graph-main-folder>
      JAVA_OPT="-Xmx8G" sbt "run <path_main-resource-folder> <name_input_folder>"

+ Cluster (once connected via ssh to the driver machine)

      spark-submit --master <master-url> --deploy-mode client --class it.unitn.bdsn.bissoli.daniele.Main --executor-memory <6+ GB> --driver-memory <6+ GB>
      
# Load data into Neo4j database
Once the processing pipeline has finished its task, the output data must be copied locally in the Neo4j import folder or anywhere is better suited. If the second choice has been taken, then the `neo4j.conf` file has to be changed in the following way:

    # comment this line as here
    #dbms.directories.import=import

and

    # set this line to true  
    dbms.security.allow_csv_import_from_file_urls=true

Now it is possible to start the Neo4j database.

Then it is necessary to prepare the environment where to run the script to load the data. So first change the working directory to the following one:

    cd <path_wikipedia-graph-main-folder>/data-visualization
    
and create a file named `.env` with the following information inside:

    NEO=<neo4j_username>
    NEO_PWD=<neo4j_password>
    NEO_URI=bolt://localhost:7687
    CSV_DATA=<path_main-resource-folder>/output/<name_input_folder>/final

**Note**: this file will contain sensitive data, so do not share it.

Then it is possible to install required libraries by running :

    npm install  

and if no errors have been received it is possible to load the data into the graph database with the following command.

    npm run load

# Run the visualization tool
To run the visualization tool, install node js required libraries as explained in the previous step, and start the Neo4j database. Then it is possible to start the visualization tool by running the below command:

    cd <path_wikipedia-graph-main-folder>/data-visualization
    npm run start
    
After a while it is possible to open the browser at this [address](http://localhost:20001) and use the visualization tool