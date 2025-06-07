package loading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class LoadingFile {

	int numOfRecordInFile;
	int numParallelWrite;
	boolean hasSyno;
	byte synoType;
	boolean writeToHDFS;
	String HDFSURI;
	String outputFolder;
	String regex;

	public LoadingFile(boolean hasSyno, boolean writeToHDFS, String HDFSURI, String inputFolder, String dsName,
			String fileName, int numOfRecordInFile, int numParallelWrite, byte synoType, String outputFolder) {

		this.hasSyno = hasSyno;
		this.writeToHDFS = writeToHDFS;
		this.HDFSURI = HDFSURI;
		this.outputFolder = outputFolder;
//		this.regex = regex;
		this.numOfRecordInFile = numOfRecordInFile;
		this.numParallelWrite = numParallelWrite;
		this.synoType = synoType;

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf sparkCond = new SparkConf().setAppName("DistributedLoading");
		sparkCond.set("spark.driver.memory", "100g");
		sparkCond.set("spark.memory.offHeap.enabled", "true");
		sparkCond.set("spark.memory.offHeap.size", "16g");
		sparkCond.set("spark.rpc.message.maxSize", "1024");
//		// Set Spark master to local if not already set
//		if (!sparkCond.contains("spark.master"))
//			sparkCond.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(sparkCond);

		String inputPath = inputFolder + fileName;
		if (!inputFolder.endsWith("/")) {
			inputPath = inputFolder + "/" + fileName;
		}
		System.out.println(inputPath);

		if (dsName.startsWith("largeWild")) {
			regex = "\t";
			loadLargeFile(inputPath, sc);
		} else if (dsName.startsWith("park")) {
			regex = ";";
			loadPark(inputPath, sc);
		}
	}

	private void loadLargeFile(String inputPath, JavaSparkContext sc) {

		// write
		System.out.println("Start writing file to distributed file system.");
		long start = 0;
		long end = 0;
		long duration = 0;

		int writeFId = 0;

		// compute synopsize size
		byte[] synopsisType = null;
		int[][] synopsisSize = { { 128, 128 }, { 256, 256 }, { 512, 512 }, { 1024, 1024 } };
//		int[][] synopsisSize = { { 128, 128 } };
//		int[][] synopsisSize = { { 1024, 1024 } };
		if (synoType == 0) {
			synopsisType = new byte[synopsisSize.length];
			for (int i = 0; i < synopsisSize.length; i++) {
				synopsisType[i] = synoType;
			}
		} else {
			synopsisType = new byte[2];
			synopsisSize = new int[2][2];
			for (int i = 0; i < 2; i++) {
				synopsisType[i] = synoType;
				synopsisSize[i][0] = 128 * (i + 1);
				synopsisSize[i][1] = 128 * (i + 1);
			}
		}

		File file = new File(inputPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			int writeCnt = 0;
			int fileId = 0;
			ArrayList<ArrayList<String>> parallelLines = new ArrayList<ArrayList<String>>();
			// first record is fileId
			ArrayList<String> tmp = new ArrayList<String>();
			tmp.add(String.valueOf(fileId));

			// wildfire header
			String line = reader.readLine();

			line = reader.readLine();

			while (line != null) {
				tmp.add(line);
				writeCnt++;

				if (writeCnt == numOfRecordInFile) {
					// file tmp to parallelLines
					parallelLines.add(tmp);
					// recover writeCnt
					writeCnt = 0;
					// increase fileId
					fileId++;
					tmp = null;
					tmp = new ArrayList<String>();
					tmp.add(String.valueOf(fileId));
				}
				if (fileId == numParallelWrite) {
					// write a file
					start = System.nanoTime();
					JavaRDD<ArrayList<String>> lineRDD = sc.parallelize(parallelLines);

//					lineRDD.foreachPartition(new WriteFunction(outputFolder, synopsisType, synopsisSize));

					if (writeToHDFS) {
						lineRDD.foreach(new WriteFileToHDFSUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
								synopsisSize, regex, writeFId));
					} else {
						lineRDD.foreach(new WriteFileToLocalUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
								synopsisSize, regex, writeFId));
					}

					end = System.nanoTime();
					duration += end - start;
					System.out.println("Do a write file, the time write in distributed file system = "
							+ (duration * 1E-9) + " s.");
					writeFId += numParallelWrite;
					System.out.println("writeFId = " + writeFId);
					// recover writeCnt and fileId
					writeCnt = 0;
					fileId = 0;
					// refill list
					parallelLines = null;
					parallelLines = new ArrayList<ArrayList<String>>();
					tmp = null;
					tmp = new ArrayList<String>();
					tmp.add(String.valueOf(fileId));
				}

				line = reader.readLine();
			}

			reader.close();

			if (parallelLines.size() > 0) {
				// write a file
				start = System.nanoTime();
				JavaRDD<ArrayList<String>> lineRDD = sc.parallelize(parallelLines);

//				lineRDD.foreachPartition(new WriteFunction(outputFolder, synopsisType, synopsisSize));

				if (writeToHDFS) {
					lineRDD.foreach(new WriteFileToHDFSUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
							synopsisSize, "\t", writeFId));
				} else {
					lineRDD.foreach(new WriteFileToLocalUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
							synopsisSize, "\t", writeFId));
				}

				end = System.nanoTime();
				duration += end - start;
				System.out.println();
				System.out.println(
						"Do a write file, the time write in distributed file system = " + (duration * 1E-9) + " s.");
				writeFId += numParallelWrite;
				System.out.println("writeFId = " + writeFId);
			}

			sc.close();
			System.out.println("Total time write in distributed file system = " + (duration * 1E-9) + " s.");
			System.out.println("Finish writing to distributed file system.");
			System.out.println("--------------------");
			System.out.println();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void loadPark(String inputPath, JavaSparkContext sc) {

		// write
		System.out.println("Start writing file to distributed file system.");
		long start = 0;
		long end = 0;
		long duration = 0;

		int writeFId = 0;
		
		byte synoType = 1;

		// compute synopsize size
		byte[] synopsisType = null;
//		int[][] synopsisSize = { { 128, 128 }, { 256, 256 }, { 512, 512 }};
		int[][] synopsisSize = { { 128, 128 }};
//					int[][] synopsisSize = { { 16, 16 }, { 32, 32 }, { 64, 64 }, { 128, 128 }};

		synopsisType = new byte[synopsisSize.length];

		for (int i = 0; i < synopsisSize.length; i++) {
			synopsisType[i] = synoType;
		}
		
		File file = new File(inputPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			int writeCnt = 0;
			int fileId = 0;
			ArrayList<ArrayList<String>> parallelLines = new ArrayList<ArrayList<String>>();
			// first record is fileId
			ArrayList<String> tmp = new ArrayList<String>();
			tmp.add(String.valueOf(fileId));

			// wildfire header
			String line = reader.readLine();

			line = reader.readLine();

			while (line != null) {
				tmp.add(line);
				writeCnt++;

				if (writeCnt == numOfRecordInFile) {
					// file tmp to parallelLines
					parallelLines.add(tmp);
					// recover writeCnt
					writeCnt = 0;
					// increase fileId
					fileId++;
					tmp = null;
					tmp = new ArrayList<String>();
					tmp.add(String.valueOf(fileId));
				}
				if (fileId == numParallelWrite) {
					// write a file
					start = System.nanoTime();
					JavaRDD<ArrayList<String>> lineRDD = sc.parallelize(parallelLines);

//					lineRDD.foreachPartition(new WriteFunction(outputFolder, synopsisType, synopsisSize));

					if (writeToHDFS) {
						lineRDD.foreach(new WriteFileToHDFSUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
								synopsisSize, regex, writeFId));
					} else {
						lineRDD.foreach(new WriteFileToLocalUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
								synopsisSize, regex, writeFId));
					}

					end = System.nanoTime();
					duration += end - start;
					System.out.println("Do a write file, the time write in distributed file system = "
							+ (duration * 1E-9) + " s.");
					writeFId += numParallelWrite;
					System.out.println("writeFId = " + writeFId);
					// recover writeCnt and fileId
					writeCnt = 0;
					fileId = 0;
					// refill list
					parallelLines = null;
					parallelLines = new ArrayList<ArrayList<String>>();
					tmp = null;
					tmp = new ArrayList<String>();
					tmp.add(String.valueOf(fileId));
				}

				line = reader.readLine();
			}

			reader.close();

			if (parallelLines.size() > 0) {
				if (tmp.size() > 0) {
					parallelLines.add(tmp);
				}
				// write a file
				start = System.nanoTime();
				JavaRDD<ArrayList<String>> lineRDD = sc.parallelize(parallelLines);

//				lineRDD.foreachPartition(new WriteFunction(outputFolder, synopsisType, synopsisSize));

				if (writeToHDFS) {
					lineRDD.foreach(new WriteFileToHDFSUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
							synopsisSize, regex, writeFId));
				} else {
					lineRDD.foreach(new WriteFileToLocalUpdate(hasSyno, HDFSURI, outputFolder, synopsisType,
							synopsisSize, regex, writeFId));
				}

				end = System.nanoTime();
				duration += end - start;
				System.out.println();
				System.out.println(
						"Do a write file, the time write in distributed file system = " + (duration * 1E-9) + " s.");
				writeFId += numParallelWrite;
				System.out.println("writeFId = " + writeFId);
			}

			sc.close();
			System.out.println("Total time write in distributed file system = " + (duration * 1E-9) + " s.");
			System.out.println("Finish writing to distributed file system.");
			System.out.println("--------------------");
			System.out.println();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String inputFolder = "/Users/xin_aurora/Downloads/Work/2019/" + "UCR/Research/Spatial/sketches/data/point/";
		String fileName = "bit031M.csv";

		boolean hasSyno = true;
		boolean writeToHDFS = false;
		String HDFSURI = "hdfs://localhost:9000";

		String dsName = "park";
		String outputFolder = inputFolder + "park/";

		int numOfRecordInFile = 10000;
		int numParallelWrite = 10;

		byte synoType = 1;

//		String regex = ",";

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-HDFSURI")) {
				HDFSURI = args[++i];
			} else if (args[i].equals("-dsName")) {
				dsName = args[++i];
			} else if (args[i].equals("-hasSyno")) {
				hasSyno = Boolean.parseBoolean(args[++i]);
			} else if (args[i].equals("-writeToHDFS")) {
				writeToHDFS = Boolean.parseBoolean(args[++i]);
			} else if (args[i].equals("-inputFolder")) {
				inputFolder = args[++i];
			} else if (args[i].equals("-outputFolder")) {
				outputFolder = args[++i];
			} else if (args[i].equals("-synoType")) {
				synoType = Byte.parseByte(args[++i]);
			} else if (args[i].equals("-numParallelWrite")) {
				numParallelWrite = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-numOfRecordInFile")) {
				numOfRecordInFile = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-fileName")) {
				fileName = args[++i];
			}
//			else if (args[i].equals("-regex")) {
//				regex = args[++i];
//			} 
		}

		LoadingFile load = new LoadingFile(hasSyno, writeToHDFS, HDFSURI, inputFolder, dsName, fileName,
				numOfRecordInFile, numParallelWrite, synoType, outputFolder);

	}

}
