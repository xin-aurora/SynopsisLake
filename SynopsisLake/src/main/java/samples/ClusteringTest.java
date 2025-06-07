package samples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import dataStructure.Tuple2;
import operator.KMeansSample;

public class ClusteringTest {
	
	double[][] sampleCenters = null;
	double[][] allDataCenters = null;
	
	public ClusteringTest(String folder, String filePath, int numOfCluster, double sampleRate) {
		
		ArrayList<Tuple2<Double, Double>> dataList = loadFile(filePath);
		UniformSamples<Tuple2<Double, Double>> uniformSamplesHelper = new UniformSamples<Tuple2<Double, Double>>(dataList.size(), sampleRate);
		
		List<Tuple2<Double, Double>> samples = uniformSamplesHelper.Sample(dataList);
		
//		System.out.println("sample size = " + samples.size());
		runKMeans(dataList, samples);
	}
	
	private void runKMeans(ArrayList<Tuple2<Double, Double>> dataList,
			List<Tuple2<Double, Double>> samples) {
		double[][] sampleClusterObject = new double[samples.size()][2];
		
		double[][] fullClusterObject = new double[dataList.size()][2];
		for (int i=0; i<dataList.size(); i++) {
			Tuple2<Double, Double> tuple =  dataList.get(i);
			fullClusterObject[i][0] = tuple.getValue0();
			fullClusterObject[i][1] = tuple.getValue1();
		}
		
		for (int i=0; i<samples.size(); i++) {
			Tuple2<Double, Double> tuple =  samples.get(i);
			sampleClusterObject[i][0] = tuple.getValue0();
			sampleClusterObject[i][1] = tuple.getValue1();
		}
		
		long start = System.nanoTime();
		KMeansSample kmeansSample = new KMeansSample(15, 200, sampleClusterObject);
		kmeansSample.clustering(false, 0);
		this.sampleCenters = kmeansSample.getCluster();
		long end = System.nanoTime();
		long sampleDuration = end - start;
		
//		System.out.println("samples centers");
//		for (int i=0; i<sampleCenters.length; i++) {
//			System.out.println(Arrays.toString(sampleCenters[i]));
//		}
//		System.out.println();
		
		start = System.nanoTime();
		KMeansSample kmeansFullObject = new KMeansSample(15, 200, fullClusterObject);
		kmeansFullObject.clustering(false, 0);
		this.allDataCenters = kmeansFullObject.getCluster();
		end = System.nanoTime();
		long allDataDuration = end - start;
		
		double sampleError = computeError(sampleCenters, fullClusterObject);
		double allDataError = computeError(allDataCenters, fullClusterObject);
		
		System.out.println("apply samples centers, error = " + sampleError + ", avg dist = " + sampleError / fullClusterObject.length);
		
		System.out.println("apply allData centers, error = " + allDataError + ", avg dist = " + allDataError / fullClusterObject.length);
	
		System.out.println("Sample data cluster time = " + (sampleDuration * 1E-9) + " s.");
		System.out.println("All data cluster time = " + (allDataDuration * 1E-9) + " s.");
	}
	
	private double computeError(double[][] centers, double[][] data) {

		double curTotalError = 0.0;

		for (int pId = 0; pId < data.length; pId++) {
			double[] curPoint = data[pId];
			double minScore = Double.MAX_VALUE;
			for (int cId = 0; cId < centers.length; cId++) {
				double score = getDis(centers[cId], curPoint);
				if (score < minScore) {
					minScore = score;
				}
			}
			curTotalError += minScore;

		}

		return curTotalError;
	}
	
	private double getDis(double[] center, double[] cell) {
		return Math.sqrt(Math.pow((center[0] - cell[0]), 2) + Math.pow((center[1] - cell[1]), 2));

	}
	
	private ArrayList<Tuple2<Double, Double>> loadFile(String filePath) {
		String[] result = null;
		try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
			result = stream.toArray(String[]::new);
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<Tuple2<Double, Double>> list = new ArrayList<Tuple2<Double, Double>>();
		
		for (String str : result) {
			String[] tmp = str.split(",");
			Tuple2<Double, Double> tuple = new Tuple2<Double, Double>(Double.parseDouble(tmp[0]),
					Double.parseDouble(tmp[1]));
			list.add(tuple);
		}
		
		return list;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/"
				+ "UCR/Research/Spatial/sketches/vldb2024/expData/point/";
		String filePath = "15clustersFull.csv";
		int numOfCluster = 15;
		double sampleRate = 0.5;
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-filePath")) {
				filePath = args[++i];
			} else if (args[i].equals("-numOfCluster")) {
				numOfCluster = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-sampleRate")) {
				sampleRate = Double.parseDouble(args[++i]);
			} 
		}
		
		filePath = folder + "15clustersFull.csv";
		
		ClusteringTest run = new ClusteringTest(folder, filePath, numOfCluster, sampleRate);
	}

}
