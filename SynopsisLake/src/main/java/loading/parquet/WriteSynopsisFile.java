package loading.parquet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import accumulator.BinaryFileSizeAccumulator;
import dataStructure.parquet.ParquetRow;
import dataStructure.parquet.ParquetRow2DCountHistogram;
import dataStructure.parquet.ParquetRowGeometricHistogram;
import dataStructure.parquet.ParquetRowUniformSamples;

public class WriteSynopsisFile {

	/**
	 * 1. Read data and write data [binary] 2. Create data synopsis and create
	 * synopsis files [binary] 3. Collects synopsis info to create synopsis metadata
	 * files [Parquet]
	 * 
	 * @param inputPath
	 * @param outputFolder
	 * @param hasHead:       whether file has header
	 * @param dataSizeBound: number of lines to process together
	 * 
	 */

	boolean loadDone = false;
	ArrayList<String> lines = new ArrayList<String>();

	String HDFSURI = "";

	public WriteSynopsisFile(String HDFSURI, String inputPath, boolean hasHead, int fileNumBound, int dataNumBound,
			boolean hasSyno, String outputFolder, int synopsisType, int[] synopsisSizes, String regex,
			double uniformSampleRate) {

		this.HDFSURI = HDFSURI;
		dataNumBound++;

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", HDFSURI);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		
		SparkConf sparkCond = new SparkConf().setAppName("DistributedLoading");
		sparkCond.set("spark.driver.memory", "100g");
//		sparkCond.set("spark.executor.memory", "100g");
		sparkCond.set("spark.memory.offHeap.enabled", "true");
		sparkCond.set("spark.memory.offHeap.size", "16g");
		sparkCond.set("spark.rpc.message.maxSize", "1024");
		// Set Spark master to local if not already set
		if (!sparkCond.contains("spark.master"))
			sparkCond.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(sparkCond);

		SparkSession spark = SparkSession.builder().config(sc.getConf()).getOrCreate();

		// Then, create an Accumulator of this type:
		BinaryFileSizeAccumulator<ParquetRow> sizeAccu = new BinaryFileSizeAccumulator<ParquetRow>();
		// Then, register it into spark context:
		sc.sc().register(sizeAccu, "sizeAccumulator");

		int writeFId = 0;
		File file = new File(inputPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			if (hasHead) {
				reader.readLine();
			}

			while (!loadDone) {
				ArrayList<ArrayList<String>> parallelLines = loadData(reader, fileNumBound, dataNumBound);
				System.out.println("Prepare to create synopses: " + parallelLines.size() + " files.");
				for (ArrayList<String> lines : parallelLines) {
					System.out.println(lines.size());
				}

				writeSynopsisFiles(sc, spark, sizeAccu, parallelLines, hasSyno, outputFolder, synopsisType,
						synopsisSizes, regex, uniformSampleRate, writeFId);

				writeFId += parallelLines.size();

			}

			reader.close();

			System.out.println("Write Data Parquet Files");
			ArrayList<ParquetRow> list = sizeAccu.value();

			String parquetFilePath = HDFSURI + "/" + outputFolder + "/parquet";
			writeParquetFile(list, spark, parquetFilePath, synopsisType);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// write data files
		// write synopsis files
		// write synopsis metadata files

		System.out.println("Start writing file to distributed file system.");
	}

	private ArrayList<String> writeSynopsisFiles(JavaSparkContext sc, SparkSession spark,
			BinaryFileSizeAccumulator<ParquetRow> sizeAccu, ArrayList<ArrayList<String>> parallelLines,
			boolean hasSyno, String outputFolder, int synopsisType, int[] synopsisSize, String regex, 
			double uniformSampleRate, int writeFId) {
		ArrayList<String> tmp = new ArrayList<String>();

		JavaRDD<ArrayList<String>> lineRDD = sc.parallelize(parallelLines);
		lineRDD.foreach(new WriteBinaryFileToHDFS(sizeAccu, hasSyno, HDFSURI, outputFolder, synopsisType, synopsisSize,
				regex, uniformSampleRate, writeFId, false));

		if (hasSyno) {
			System.out.println("Write Synopses Binary Files");
		}

		return tmp;
	}

	private void writeParquetFile(ArrayList<ParquetRow> list, SparkSession spark, String parquetFilePath,
			int synopsisType) {
		
		switch (synopsisType) {
		case -1:
			Dataset<Row> df = spark.sqlContext().createDataFrame(list, ParquetRow.class);
			df.coalesce(1).write().parquet(parquetFilePath);
			break;
		case 0:
			// 1D Count histogram
			break;
		case 1:
			// 2D Count histogram
			Dataset<Row> histDF = spark.sqlContext().createDataFrame(list, ParquetRow2DCountHistogram.class);
			histDF.coalesce(1).write().parquet(parquetFilePath);
			break;
		case 4:
			// Geometric Histogram
			Dataset<Row> geohistDF = spark.sqlContext().createDataFrame(list, ParquetRowGeometricHistogram.class);
			geohistDF.coalesce(1).write().parquet(parquetFilePath);
			break;
		case 6:
			// Uniform Samples
			Dataset<Row> usDF = spark.sqlContext().createDataFrame(list, ParquetRowUniformSamples.class);
			usDF.coalesce(1).write().parquet(parquetFilePath);
			break;
		}

	}

	private ArrayList<ArrayList<String>> loadData(BufferedReader reader, int fileNumBound, int dataNumBound) {
		System.out.println("Start loading files");

		ArrayList<ArrayList<String>> parallelLines = new ArrayList<ArrayList<String>>();
		try {

			int fileId = 0;
			if (lines.size() == 0) {
				lines.add(String.valueOf(fileId));
			}
			while (parallelLines.size() < fileNumBound) {
				String line = reader.readLine();
				while (lines.size() < dataNumBound) {
					lines.add(line);
					line = reader.readLine();
					if (line == null) {
						parallelLines.add(lines);
						lines = null;
						loadDone = true;
//						System.out.println(lines);
						return parallelLines;
					}
				}
				if (lines.size() == dataNumBound) {
					parallelLines.add(lines);
					lines = new ArrayList<String>();
					if (parallelLines.size() == fileNumBound) {
						lines.add(String.valueOf(0));
						lines.add(line);
					} else {
						fileId++;
						lines.add(String.valueOf(fileId));
						lines.add(line);
					}
				}

			}
//			System.out.println(lines);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return parallelLines;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fileFolder = "/Users/xin_aurora/Downloads/Work/2019/" 
				+ "UCR/Research/Spatial/sketches/data/fileFormat/";
		String inputPath = fileFolder + "osm21_pois_coords_sample.csv";
		boolean hasHead = true;
		int fileNumBound = 5;
		int dataNumBound = 100000;

		String HDFSURI = "hdfs://localhost:9000";

		boolean hasSyno = true;
//		String outputFolder = "/geoHist/";
		int synopsisType = 4;
		String outputFolder = "/ParuetFile/";
//		int synopsisType = 1;
		String synopsisSizesStr = "10,10";
		String regex = ",";
		double uniformSampleRate = 0.001;

		String[] synoSizesStrTmp = synopsisSizesStr.split(",");
		int[] synopsisSizes = new int[synoSizesStrTmp.length];
		for (int i = 0; i < synoSizesStrTmp.length; i++) {
			synopsisSizes[i] = Integer.parseInt(synoSizesStrTmp[i].trim());
		}

		WriteSynopsisFile writerFile = new WriteSynopsisFile(HDFSURI, inputPath, hasHead, fileNumBound, dataNumBound,
				hasSyno, outputFolder, synopsisType, synopsisSizes, regex, uniformSampleRate);
		
//		ArrayList<Integer> list = new ArrayList<Integer>();
//		for(int i=0; i<10; i++) {
//			list.add(i);
//		}
//		
//		Integer[] array = new Integer[list.size()];
//		list.toArray(array);
//		System.out.println(Arrays.toString(array));
	}

}
