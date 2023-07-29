package dataStructure.objFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import histogram.SimpleSpatialHistogramOpt;
import utils.UtilsFunction;
import utils.UtilsFunctionHistogram;

public class Test {

	public static void BuildData(String folder, String inputName, String outputName, double min, double max,
			int numOfRepeat) {

		File file = new File(folder + inputName);
		double[] x = new double[100];
		double[] y = new double[100];

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			int cnt = 0;
			while (line != null) {
				String[] tmp = line.split(",");
				x[cnt] = Double.parseDouble(tmp[0]);
				y[cnt] = Double.parseDouble(tmp[1]);
				cnt++;
				line = reader.readLine();
			}
			reader.close();

			double range = max - min;

			FileWriter writer = new FileWriter(folder + outputName);
			for (int i = 0; i < numOfRepeat; i++) {
				for (int idx = 0; idx < x.length; idx++) {
					String str = (x[idx] * range + min) + "," + (y[idx] * range + min) + "\n";
					writer.write(str);
				}
			}
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static double[][] Load(String path, int numItem) {
//		int cnt = 0;
//		int cnt2= 0;
//		int cnt3 = 0;
//		int cnt4 = 0;
		double[][] data = new double[numItem][2];

		File file = new File(path);

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			int idx = 0;
			while (line != null) {
				String[] tmp = line.split(",");
				double[] pos = new double[2];
				pos[0] = Double.parseDouble(tmp[0]);
				pos[1] = Double.parseDouble(tmp[1]);
//				if (UtilsFunction.isOverlapPointCell(0, 0, 2, 2, pos[0], pos[1])) {
//					cnt++;
//				}
//				if (UtilsFunction.isOverlapPointCell(20, 20, 22, 22, pos[0], pos[1])) {
//					cnt2++;
//				}
//				if (UtilsFunction.isOverlapPointCell(30, 30, 32, 32, pos[0], pos[1])) {
//					cnt3++;
//				}
//				if (UtilsFunction.isOverlapPointCell(40, 40, 45, 45, pos[0], pos[1])) {
//					cnt4++;
//				}
				data[idx] = pos;
				idx++;
				line = reader.readLine();
			}
			reader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		System.out.println("0-0-2-2 ans = " + cnt);
//		System.out.println("20-20-22-22 ans = " + cnt2);
//		System.out.println("30-30-32-32 ans = " + cnt3);
//		System.out.println("40-40-45-45 ans = " + cnt4);

		return data;

	}

	public static void AggPrecision(String[] paths, String gtPath) {

		double min = 0;
		double max = 55;
		
//		// VMeasure
//		double[] partition = {0.0, 15.0, 30.0, 55.0};
		// obj function
		double[] partition = { 0.0, 20.0, 35.0, 55.0 };

		double[][] data1 = Load(paths[0], 1200);
		double[][] data2 = Load(paths[1], 900);
		double[][] data3 = Load(paths[2], 6000);
		double[][] data4 = Load(paths[3], 4500);
//		for (int i=0; i<data1.length; i++) {
//			System.out.println(Arrays.toString(data1[i]));
//		}

//		SimpleSpatialHistogramOpt[] sources = new SimpleSpatialHistogramOpt[4];
		SimpleSpatialHistogramOpt src1 = new SimpleSpatialHistogramOpt(0, 0, 20, 20, 10, 10);
		src1 = src1.loadHistByDataArray(src1, data1);
		SimpleSpatialHistogramOpt src2 = new SimpleSpatialHistogramOpt(15, 15, 30, 30, 10, 10);
		src2 = src2.loadHistByDataArray(src2, data2);
		SimpleSpatialHistogramOpt src3 = new SimpleSpatialHistogramOpt(20, 20, 35, 35, 10, 10);
		src3 = src3.loadHistByDataArray(src3, data3);
		SimpleSpatialHistogramOpt src4 = new SimpleSpatialHistogramOpt(30, 30, 55, 55, 10, 10);
		src4 = src4.loadHistByDataArray(src4, data4);

//		double[][] data = src4.getData();
//		for (int i=0; i<data.length; i++) {
//			System.out.println(Arrays.toString(data[i]));
//		}

//		 double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
		SimpleSpatialHistogramOpt uniform = new SimpleSpatialHistogramOpt(min, min, max, max, 10, 10);
		uniform.aggregateHistogram(src1);
		uniform.aggregateHistogram(src2);
		uniform.aggregateHistogram(src3);
		uniform.aggregateHistogram(src4);

		SimpleSpatialHistogramOpt uniformGT = new SimpleSpatialHistogramOpt(min, min, max, max, 10, 10);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data1);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data2);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data3);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data4);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(uniformGT, uniform);

		int[] bounds = { 14, 83, 3 };
		double[] p1 = { 0.0, 20.0 };
		double[] p2 = { 20.0, 35.0 };
		double[] p3 = { 35.0, 55.0 };
		
//		// VMeasure
//		double[] partition = {0.0, 15.0, 30.0, 55.0};

		SimpleSpatialHistogramOpt t1 = new SimpleSpatialHistogramOpt(0, 0, 20, 20, 4, 4);
		t1.aggregateHistogram(src1);
		t1.aggregateHistogram(src2);

		SimpleSpatialHistogramOpt t1GT = new SimpleSpatialHistogramOpt(0, 0, 20, 20, 4, 4);
		t1GT = t1GT.loadHistByDataArray(t1GT, data1);
		t1GT = t1GT.loadHistByDataArray(t1GT, data2);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(t1GT, t1);

		SimpleSpatialHistogramOpt t2 = new SimpleSpatialHistogramOpt(20, 20, 35, 35, 9, 9);
		t2.aggregateHistogram(src2);
		t2.aggregateHistogram(src3);
		t2.aggregateHistogram(src4);

		SimpleSpatialHistogramOpt t2GT = new SimpleSpatialHistogramOpt(20, 20, 35, 35, 9, 9);
		t2GT = t2GT.loadHistByDataArray(t2GT, data2);
		t2GT = t2GT.loadHistByDataArray(t2GT, data3);
		t2GT = t2GT.loadHistByDataArray(t2GT, data4);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(t2GT, t2);

//		double[][] data = t2.getData();
//		for (int i = 0; i < data.length; i++) {
//			System.out.println(Arrays.toString(data[i]));
//		}
//
//		data = t2GT.getData();
//		for (int i = 0; i < data.length; i++) {
//			System.out.println(Arrays.toString(data[i]));
//		}

		SimpleSpatialHistogramOpt t3 = new SimpleSpatialHistogramOpt(35, 35, 55, 55, 2, 2);
		t3.aggregateHistogram(src4);

		SimpleSpatialHistogramOpt t3GT = new SimpleSpatialHistogramOpt(35, 35, 55, 55, 2, 2);
		t3GT = t3GT.loadHistByDataArray(t3GT, data4);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(t3GT, t3);
		
		TestQuery(gtPath, uniform, t2);

	}
	
	public static void AggPrecisionVMeasure(String[] paths, String gtPath) {

		double min = 0;
		double max = 55;
		
//		// VMeasure
//		double[] partition = {0.0, 15.0, 30.0, 55.0};
		// obj function
//		double[] partition = { 0.0, 20.0, 35.0, 55.0 };

		double[][] data1 = Load(paths[0], 1200);
		double[][] data2 = Load(paths[1], 900);
		double[][] data3 = Load(paths[2], 6000);
		double[][] data4 = Load(paths[3], 4500);

//		SimpleSpatialHistogramOpt[] sources = new SimpleSpatialHistogramOpt[4];
		SimpleSpatialHistogramOpt src1 = new SimpleSpatialHistogramOpt(0, 0, 20, 20, 10, 10);
		src1 = src1.loadHistByDataArray(src1, data1);
		SimpleSpatialHistogramOpt src2 = new SimpleSpatialHistogramOpt(15, 15, 30, 30, 10, 10);
		src2 = src2.loadHistByDataArray(src2, data2);
		SimpleSpatialHistogramOpt src3 = new SimpleSpatialHistogramOpt(20, 20, 35, 35, 10, 10);
		src3 = src3.loadHistByDataArray(src3, data3);
		SimpleSpatialHistogramOpt src4 = new SimpleSpatialHistogramOpt(30, 30, 55, 55, 10, 10);
		src4 = src4.loadHistByDataArray(src4, data4);

//		 double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
		SimpleSpatialHistogramOpt uniform = new SimpleSpatialHistogramOpt(min, min, max, max, 10, 10);
		uniform.aggregateHistogram(src1);
		uniform.aggregateHistogram(src2);
		uniform.aggregateHistogram(src3);
		uniform.aggregateHistogram(src4);

		SimpleSpatialHistogramOpt uniformGT = new SimpleSpatialHistogramOpt(min, min, max, max, 10, 10);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data1);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data2);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data3);
		uniformGT = uniformGT.loadHistByDataArray(uniformGT, data4);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(uniformGT, uniform);

		int[] bounds = { 14, 83, 3 };
		double[] p1 = { 0.0, 20.0 };
		double[] p2 = { 20.0, 35.0 };
		double[] p3 = { 35.0, 55.0 };
		
//		// VMeasure
//		double[] partition = {0.0, 15.0, 30.0, 55.0};

		SimpleSpatialHistogramOpt t1 = new SimpleSpatialHistogramOpt(0, 0, 15, 15, 4, 4);
		t1.aggregateHistogram(src1);
//		t1.aggregateHistogram(src2);

		SimpleSpatialHistogramOpt t1GT = new SimpleSpatialHistogramOpt(0, 0, 15, 15, 4, 4);
		t1GT = t1GT.loadHistByDataArray(t1GT, data1);
//		t1GT = t1GT.loadHistByDataArray(t1GT, data2);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(t1GT, t1);

		SimpleSpatialHistogramOpt t2 = new SimpleSpatialHistogramOpt(15, 15, 30, 30, 9, 9);
		t2.aggregateHistogram(src2);
		t2.aggregateHistogram(src3);
//		t2.aggregateHistogram(src4);

		SimpleSpatialHistogramOpt t2GT = new SimpleSpatialHistogramOpt(15, 15, 30, 30, 9, 9);
		t2GT = t2GT.loadHistByDataArray(t2GT, data2);
		t2GT = t2GT.loadHistByDataArray(t2GT, data3);
//		t2GT = t2GT.loadHistByDataArray(t2GT, data4);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(t2GT, t2);

		SimpleSpatialHistogramOpt t3 = new SimpleSpatialHistogramOpt(30, 30, 55, 55, 2, 2);
		t3.aggregateHistogram(src4);

		SimpleSpatialHistogramOpt t3GT = new SimpleSpatialHistogramOpt(30, 30, 55, 55, 2, 2);
		t3GT = t3GT.loadHistByDataArray(t3GT, data4);

		UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(t3GT, t3);
		
		TestQuery(gtPath, uniform, t2);

	}
	
	public static void TestQuery (String queryPath, SimpleSpatialHistogramOpt uniform, SimpleSpatialHistogramOpt t2) {
		File file = new File(queryPath);
		double[][] query = new double[1000][3];
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			int idx = 0;
			while (line != null) {
				String[] tmp = line.split(",");
				double[] pos = new double[3];
				pos[0] = Double.parseDouble(tmp[0]);
				pos[1] = Double.parseDouble(tmp[1]);
				pos[2] = Double.parseDouble(tmp[2]);
				query[idx] = pos;
				idx++;
				line = reader.readLine();
			}
			reader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		double relErrorUniform = 0.0;
		double absErrorUniform = 0.0;
		
		double relErrorObj = 0.0;
		double absErrorObj = 0.0;
		
		for (int i=0; i<query.length; i++) {
			double qMin = query[i][0];
			double qMax = query[i][1];
			double qAnsGT = query[i][2];
			
			double uniformAns = uniform.RangeQuery(qMin, qMax, qMin, qMax);
			double densityAns = t2.RangeQuery(qMin, qMax, qMin, qMax);
			
			if (qAnsGT == 0) {
//				absErrorUniform += uniformAns;
//				absErrorObj += densityAns;
//				
//				relErrorUniform += uniformAns;
//				relErrorObj += densityAns;
				
			} else {
				absErrorUniform += Math.abs(uniformAns - qAnsGT);
				absErrorObj += Math.abs(densityAns - qAnsGT);
				
				relErrorUniform += Math.abs(uniformAns - qAnsGT) / qAnsGT;
				relErrorObj += Math.abs(densityAns - qAnsGT) / qAnsGT;
			}
		}
		
		System.out.println("total absErrorUniform = " + absErrorUniform + ", avg = " + absErrorUniform / 1000);
		System.out.println("total absErrorObj = " + absErrorObj + ", avg = " + absErrorObj / 1000);
		System.out.println("total relErrorUniform = " + relErrorUniform + ", avg = " + relErrorUniform / 1000);
		System.out.println("total relErrorObj = " + relErrorObj + ", avg = " + relErrorObj / 1000);
	}

	public static void QueryGenerator(String queryPath, int numOfQuery) {
		Random rand = new Random();
		double[] mins = new double[numOfQuery];
		double[] maxs = new double[numOfQuery];
		int cnt = 0;
		while (cnt < numOfQuery) {
			// Min + (Math.random() * (Max - Min))
			double min = rand.nextDouble() * (30 - 20) + 20;
			double max = min + 2;
			mins[cnt] = min;
			maxs[cnt] = max;
			cnt++;
		}

		try {
			FileWriter query = new FileWriter(queryPath);
			for (int i = 0; i < mins.length; i++) {
				String str = mins[i] + "," + maxs[i] + "\n";
				query.write(str);
			}
			query.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void GetGT(String queryPath, String gtPath, String folder) {
		File file = new File(queryPath);
		double[][] query = new double[1000][2];
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();
			int idx = 0;
			while (line != null) {
				String[] tmp = line.split(",");
				double[] pos = new double[2];
				pos[0] = Double.parseDouble(tmp[0]);
				pos[1] = Double.parseDouble(tmp[1]);
				query[idx] = pos;
				idx++;
				line = reader.readLine();
			}
			reader.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		double[][] data2 = Load(folder + "1.csv", 900);
		double[][] data3 = Load(folder + "2.csv", 6000);

		try {
			FileWriter GTFile = new FileWriter(gtPath);
			for (int i = 0; i < query.length; i++) {
				double qMin = query[i][0];
				double qMax = query[i][1];
				int gtValue = 0;
				for (int idx1 = 0; idx1 < data2.length; idx1++) {
					if (UtilsFunction.isOverlapPointCell
							(qMin, qMin, qMax, qMax, data2[idx1][0], data2[idx1][1])) {
						gtValue++;
					}
				}
				for (int idx2 = 0; idx2 < data3.length; idx2++) {
					if (UtilsFunction.isOverlapPointCell
							(qMin, qMin, qMax, qMax, data3[idx2][0], data3[idx2][1])) {
						gtValue++;
					}
				}
				String str = query[i][0] + "," + query[i][1] + "," + gtValue + "\n";
				GTFile.write(str);
			}
			GTFile.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/exp/objFunction/";
//		String inputName = "uniform.csv";
//		int fileId = 3;
//		String outputName = fileId + ".csv";
//		
//		BuildData(folder, inputName, outputName, 30, 55, 45);

		String[] paths = new String[4];
		for (int i = 0; i < paths.length; i++) {
			paths[i] = folder + i + ".csv";
		}
		String gtPath = folder + "queryGT.txt";
		AggPrecision(paths, gtPath);
//		AggPrecisionVMeasure(paths, gtPath);
//		String queryPath = folder + "query.txt";
//		QueryGenerator(queryPath, 1000);

//		String queryPath = folder + "query.txt";
//		String gtPath = folder + "queryGT.txt";
//
//		GetGT(queryPath, gtPath, folder);
	}

}
