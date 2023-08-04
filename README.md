# SynopsisLake
The is a temporary repository for ICDE 2024 submission

# Query Throughput Experiment Results Correction



To run the loading experiment:

Please first compile the project:
	
 mvn package

Then run:

spark-submit --class SynopsisLake.SynopsisOverheadExp ./target/SynopsisLake-0.0.1-SNAPSHOT.jar -HDFSURI hdfs://REPLACE_WITH_YOUR_HDFS_URI -datasetPath osm21_pois_shuffle_sample.csv -expName loadNoSyno -synopsisFolder countHistogram -synopsisType 1 -synopsisResolution 128 -numFile 10 -numOfRecordPerFile 30000

Or open the source code to run SynopsisOverheadExp.java

The path to the file: 

/src/main/java/SynopsisLake/SynopsisOverheadExp.java
