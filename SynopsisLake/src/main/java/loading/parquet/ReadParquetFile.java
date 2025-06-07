package loading.parquet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import dataStructure.parquet.ParquetRowUniformSamples;
import scala.collection.mutable.WrappedArray;

public class ReadParquetFile {

	public static void readOneSynopsisFile() {
		System.out.println("single files");
		String path = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/vldb2024/expResult/efficiency/synopsisLake.snappy.parquet";
//		String path = "hdfs://localhost:9000//writeOpt/syno-100";
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().appName("ParquetLoadingExample").config("spark.master", "local")
				.getOrCreate();

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
//		Dataset<Row> parquetFileDF = spark.read().parquet(path);
		Dataset<Row> parquetFileDF = spark.read().parquet(path);
//		parquetFileDF.show();
		parquetFileDF.printSchema();
//		System.out.println(parquetFileDF.head());

//		parquetFileDF.createOrReplaceTempView("parquetFile");
//		Dataset<Row> resultDF = spark.sql("SELECT sampleData FROM parquetFile");
////
//		List<Row> rows = resultDF.collectAsList();
//		System.out.println(rows.size());
//		ArrayList<double[]> samples = new ArrayList<double[]>();
//		Row r = rows.get(0);
////		System.out.println(r);
//		WrappedArray<WrappedArray<Double>> wrappedArray = (WrappedArray<WrappedArray<Double>>) r.get(0);
//
//		for (int j = 0; j < wrappedArray.size(); j++) {
//			WrappedArray<Double> innerWrappedArray = wrappedArray.apply(j);
//			double[] innerResult = new double[innerWrappedArray.size()];
////			System.out.println("innerWrappedArray.size() = " + innerWrappedArray.size());
//			for (int k = 0; k < innerWrappedArray.size(); k++) {
//				innerResult[k] = (double) innerWrappedArray.apply(k);
//			}
//			samples.add(innerResult);
//		}
//		System.out.println(samples.size());
////		for (int i=0; i<samples.size(); i++) {
////			System.out.println(Arrays.toString(samples.get(i)));
////		}
//		System.out.println(Arrays.toString(samples.get(0)));
//		System.out.println(Arrays.toString(samples.get(10)));
//		System.out.println(Arrays.toString(samples.get(100)));
		spark.stop();
	}
	
	public static void readMultipleSynosisFile() {
		System.out.println("multiple files");
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().appName("ParquetLoadingExample").config("spark.master", "local")
				.getOrCreate();
		String path = "hdfs://localhost:9000//synopsisLake/syno-";
		ArrayList<double[]> samples = new ArrayList<double[]>();
		for (int i = 0; i < 2; i++) {
			int fId = i * 2 + 2;
			String synopsisPath = path + fId;
			Dataset<Row> parquetFileDF = spark.read().parquet(synopsisPath);

			parquetFileDF.createOrReplaceTempView("parquetFile");
			Dataset<Row> resultDF = spark.sql("SELECT sampleData FROM parquetFile");
			//
			List<Row> rows = resultDF.collectAsList();
//			System.out.println("rows size = " + rows.size());
			int rowSize = rows.size();
			for (int rId = 0; rId < rowSize; rId++) {
				Row r = rows.get(rId);
//				System.out.println(r);
				WrappedArray<WrappedArray<Double>> wrappedArray = (WrappedArray<WrappedArray<Double>>) r.get(0);

				for (int j = 0; j < wrappedArray.size(); j++) {
					WrappedArray<Double> innerWrappedArray = wrappedArray.apply(j);
					double[] innerResult = new double[innerWrappedArray.size()];
					for (int k = 0; k < innerWrappedArray.size(); k++) {
						innerResult[k] = (double) innerWrappedArray.apply(k);
					}
					samples.add(innerResult);
				}
			}
//			System.out.println(samples.size());
		}

		System.out.println(samples.size());
		for (int i = 0; i < samples.size(); i++) {
			System.out.println(Arrays.toString(samples.get(i)));
		}

		spark.close();
	}

	public static void readOneHistogramFile() {
		System.out.println("single files");
		String path = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/vldb2024/expResult/efficiency/syno-4";
//		String path = "hdfs://localhost:9000//writeOpt/syno-100";
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().appName("ParquetLoadingExample").config("spark.master", "local")
				.getOrCreate();

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
//		Dataset<Row> parquetFileDF = spark.read().parquet(path);
		Dataset<Row> parquetFileDF = spark.read().parquet(path);
//		parquetFileDF.show();
		parquetFileDF.printSchema();
//		System.out.println(parquetFileDF.head());

		// public SimpleSpatialHistogramOpt(double minLon, double minLat, double maxLon,
		// double maxLat,
		// int numLonBucket, int numLatBucket, double[] lons, double[] lats) {
//		parquetFileDF.createOrReplaceTempView("parquetFile");
//		Dataset<Row> rangeDF = spark.sql("SELECT range FROM parquetFile");
//		List<Row> rangeRows = rangeDF.collectAsList();
//		Row rangeR = rangeRows.get(0);
//		System.out.println(rangeR);
//		WrappedArray<Double> wrappedArrayRange = (WrappedArray<Double>) rangeR.get(0);
//		double[] ranges = new double[wrappedArrayRange.size()];
//		for (int k = 0; k < wrappedArrayRange.size(); k++) {
//			ranges[k] = (double) wrappedArrayRange.apply(k);
//		}
//		System.out.println(Arrays.toString(ranges));
//		Dataset<Row> dimensionDF = spark.sql("SELECT dimensionSizes FROM parquetFile");
//		List<Row> dimensionRows = dimensionDF.collectAsList();
//		Row dimensionR = dimensionRows.get(0);
//		System.out.println(dimensionR);
//		WrappedArray<Integer> wrappedArrayDim = (WrappedArray<Integer>) dimensionR.get(0);
//		int[] dimensions = new int[wrappedArrayDim.size()];
//		for (int k = 0; k < wrappedArrayDim.size(); k++) {
//			dimensions[k] = (int) wrappedArrayDim.apply(k);
//		}
//		System.out.println(Arrays.toString(dimensions));
//		Dataset<Row> shapeDF = spark.sql("SELECT shape FROM parquetFile");
//		List<Row> shapeRows = shapeDF.collectAsList();
//		Row shapeR = shapeRows.get(0);
//		System.out.println(shapeR);
//		WrappedArray<Double> wrappedArrayShape = (WrappedArray<Double>) shapeR.get(0);
//		double[] shapes = new double[wrappedArrayShape.size()];
//		for (int k = 0; k < wrappedArrayShape.size(); k++) {
//			shapes[k] = (double) wrappedArrayShape.apply(k);
//		}
//		System.out.println(Arrays.toString(shapes));
//		double[] lonBoundary = new double[dimensions[0] + 1];
//		double[] latBoundary = new double[dimensions[1] + 1];
//		for (int i = 0; i < dimensions[0]; i++) {
//			lonBoundary[i] = ranges[0] + i * shapes[0];
//		}
//		lonBoundary[dimensions[0]] = ranges[1];
//		for (int i = 0; i < dimensions[1]; i++) {
//			latBoundary[i] = ranges[2] + i * shapes[1];
//		}
//		latBoundary[dimensions[1]] = ranges[3];
//		System.out.println(Arrays.toString(lonBoundary));
//		System.out.println(Arrays.toString(latBoundary));
//		double[][] data = new double[dimensions[0]][dimensions[1]];
//		Dataset<Row> dataDF = spark.sql("SELECT counts FROM parquetFile");
//		List<Row> dataRows = dataDF.collectAsList();
//		Row dataR = dataRows.get(0);
//		System.out.println(dataR);
//		WrappedArray<WrappedArray<Double>> wrappedArray = (WrappedArray<WrappedArray<Double>>) dataR.get(0);
//		for (int j = 0; j < wrappedArray.size(); j++) {
//			WrappedArray<Double> innerWrappedArray = wrappedArray.apply(j);
//			for (int k = 0; k < innerWrappedArray.size(); k++) {
//				data[j][k] = (double) innerWrappedArray.apply(k);
//			}
//		}
//		for (int i=0; i<data.length; i++) {
//			System.out.println(Arrays.toString(data[i]));
//		}
		spark.stop();
	}
	
	public static void readMultipleHistogramFile() {
		System.out.println("multiple files");
		String path = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/vldb2024/expResult/efficiency/syno-4";
//		String path = "hdfs://localhost:9000//writeOpt/syno-100";
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().appName("ParquetLoadingExample").config("spark.master", "local")
				.getOrCreate();

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
//		Dataset<Row> parquetFileDF = spark.read().parquet(path);
		Dataset<Row> parquetFileDF = spark.read().parquet(path);
//		parquetFileDF.show();
		parquetFileDF.printSchema();
//		System.out.println(parquetFileDF.head());

		// public SimpleSpatialHistogramOpt(double minLon, double minLat, double maxLon,
		// double maxLat,
		// int numLonBucket, int numLatBucket, double[] lons, double[] lats) {
		parquetFileDF.createOrReplaceTempView("parquetFile");
		Dataset<Row> rangeDF = spark.sql("SELECT range FROM parquetFile");
		List<Row> rangeRows = rangeDF.collectAsList();
		Dataset<Row> dimensionDF = spark.sql("SELECT dimensionSizes FROM parquetFile");
		List<Row> dimensionRows = dimensionDF.collectAsList();
		Dataset<Row> shapeDF = spark.sql("SELECT shape FROM parquetFile");
		List<Row> shapeRows = shapeDF.collectAsList();
		Dataset<Row> dataDF = spark.sql("SELECT counts FROM parquetFile");
		List<Row> dataRows = dataDF.collectAsList();
		
		int rowSize = rangeRows.size();
		for (int rId = 0; rId < rowSize; rId++) {
			System.out.println("rId = " + rId);
			Row rangeR = rangeRows.get(rId);
			System.out.println(rangeR);
			WrappedArray<Double> wrappedArrayRange = (WrappedArray<Double>) rangeR.get(0);
			double[] ranges = new double[wrappedArrayRange.size()];
			for (int k = 0; k < wrappedArrayRange.size(); k++) {
				ranges[k] = (double) wrappedArrayRange.apply(k);
			}
			System.out.println(Arrays.toString(ranges));
			
			Row dimensionR = dimensionRows.get(rId);
			System.out.println(dimensionR);
			WrappedArray<Integer> wrappedArrayDim = (WrappedArray<Integer>) dimensionR.get(0);
			int[] dimensions = new int[wrappedArrayDim.size()];
			for (int k = 0; k < wrappedArrayDim.size(); k++) {
				dimensions[k] = (int) wrappedArrayDim.apply(k);
			}
			System.out.println(Arrays.toString(dimensions));
			
			Row shapeR = shapeRows.get(rId);
			System.out.println(shapeR);
			WrappedArray<Double> wrappedArrayShape = (WrappedArray<Double>) shapeR.get(0);
			double[] shapes = new double[wrappedArrayShape.size()];
			for (int k = 0; k < wrappedArrayShape.size(); k++) {
				shapes[k] = (double) wrappedArrayShape.apply(k);
			}
			System.out.println(Arrays.toString(shapes));
			double[] lonBoundary = new double[dimensions[0] + 1];
			double[] latBoundary = new double[dimensions[1] + 1];
			for (int i = 0; i < dimensions[0]; i++) {
				lonBoundary[i] = ranges[0] + i * shapes[0];
			}
			lonBoundary[dimensions[0]] = ranges[1];
			for (int i = 0; i < dimensions[1]; i++) {
				latBoundary[i] = ranges[2] + i * shapes[1];
			}
			latBoundary[dimensions[1]] = ranges[3];
			System.out.println(Arrays.toString(lonBoundary));
			System.out.println(Arrays.toString(latBoundary));
			double[][] data = new double[dimensions[0]][dimensions[1]];
			
			Row dataR = dataRows.get(rId);
			System.out.println(dataR);
			WrappedArray<WrappedArray<Double>> wrappedArray = (WrappedArray<WrappedArray<Double>>) dataR.get(0);
			for (int j = 0; j < wrappedArray.size(); j++) {
				WrappedArray<Double> innerWrappedArray = wrappedArray.apply(j);
				for (int k = 0; k < innerWrappedArray.size(); k++) {
					data[j][k] = (double) innerWrappedArray.apply(k);
				}
			}
			for (int i=0; i<data.length; i++) {
				System.out.println(Arrays.toString(data[i]));
			}
			
			System.out.println();
		}
		
		
		spark.stop();
	}

	public static void main(String[] args) {
		readOneHistogramFile();
//		readMultipleHistogramFile();
	}

}
