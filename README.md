# SynopsisLake
The is a temporary repository for ICDE 2024 submission

# Query Throughput Experiment Results Correction

Below is the correction of query throughput (Figure.5) in the experiment section.
<img width="1259" alt="QueryThroughput_Figure5" src="https://github.com/xin-aurora/SynopsisLake/assets/56269194/1108b2cb-2e12-4e22-841b-a48abcf7ba4a">

Compared with PartialSearch, SynopsisLake can improve the query throughput 5x to 102x.
We upload the result files in ./Throughput_Result_Correction/... Please check the *.xlsx files for details.

## Run experiment
To run the loading experiment:

Please first compile the project:
	
 mvn package

Then run:

spark-submit --class SynopsisLake.SynopsisOverheadExp ./target/SynopsisLake-0.0.1-SNAPSHOT.jar -HDFSURI hdfs://REPLACE_WITH_YOUR_HDFS_URI -datasetPath osm21_pois_shuffle_sample.csv -expName loadNoSyno -synopsisFolder countHistogram -synopsisType 1 -synopsisResolution 128 -numFile 10 -numOfRecordPerFile 30000

Or open the source code to run SynopsisOverheadExp.java

The path to the file: 

/src/main/java/SynopsisLake/SynopsisOverheadExp.java
