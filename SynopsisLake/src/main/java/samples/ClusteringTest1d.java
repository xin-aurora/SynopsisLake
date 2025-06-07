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
import operator.KMeansSample1D;

public class ClusteringTest1d {
	
	double[] sampleCenters = null;
	double[] allDataCenters = null;
	
	public ClusteringTest1d(String folder, String filePath, int numOfCluster, double sampleRate) {
		
		ArrayList<Double> dataList = loadFile(filePath);
		UniformSamples<Double> uniformSamplesHelper = new UniformSamples<Double>(dataList.size(), sampleRate);
		
		List<Double> samples = uniformSamplesHelper.Sample(dataList);
		
//		System.out.println("sample size = " + samples.size());
		runKMeans(dataList, samples);
	}
	
	private void runKMeans(ArrayList<Double> dataList,
			List<Double> samples) {
		double[][] sampleClusterObject = new double[samples.size()][2];
		
		double[][] fullClusterObject = new double[dataList.size()][2];
		for (int i=0; i<dataList.size(); i++) {
			double tuple =  dataList.get(i);
			fullClusterObject[i][0] = tuple;
		}
		
		for (int i=0; i<samples.size(); i++) {
			double tuple =  samples.get(i);
			sampleClusterObject[i][0] = tuple;
		}
		
		long start = System.nanoTime();
		KMeansSample1D kmeansSample = new KMeansSample1D(15, 200, sampleClusterObject);
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
		KMeansSample1D kmeansFullObject = new KMeansSample1D(15, 200, fullClusterObject);
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
	
	private double computeError(double[] centers, double[][] data) {

		double curTotalError = 0.0;

		for (int pId = 0; pId < data.length; pId++) {
			double curPoint = data[pId][0];
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
	
	private double getDis(double center, double cell) {
		return Math.sqrt(Math.pow((center - cell), 2));

	}
	
	private ArrayList<Double> loadFile(String filePath) {
		String[] result = null;
		try (Stream<String> stream = Files.lines(Paths.get(filePath))) {
			result = stream.toArray(String[]::new);
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<Double> list = new ArrayList<Double>();
		
		for (String str : result) {
			String[] tmp = str.split(",");
			list.add(Double.parseDouble(tmp[0]));
		}
		
		return list;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/"
				+ "UCR/Research/Spatial/sketches/vldb2024/expData/point/"
				+ "small/";
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
		
		ClusteringTest1d run = new ClusteringTest1d(folder, filePath, numOfCluster, sampleRate);
	}

}
