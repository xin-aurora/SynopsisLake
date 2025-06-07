package samples;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;


import dataStructure.Tuple2;
import operator.KMeansSample;

public class ClusteringTestK {

	double[][] allDataCenters = null;

	public ClusteringTestK(String folder, String filePath, int maxNumCluster, String outputPath) {
		ArrayList<Tuple2<Double, Double>> dataList = loadFile(filePath);

		runKMeans(dataList, outputPath, maxNumCluster);
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

	private void runKMeans(ArrayList<Tuple2<Double, Double>> dataList, String outputPath, int maxNumCluster) {

		double[][] fullClusterObject = new double[dataList.size()][2];
		for (int i = 0; i < dataList.size(); i++) {
			Tuple2<Double, Double> tuple = dataList.get(i);
			fullClusterObject[i][0] = tuple.getValue0();
			fullClusterObject[i][1] = tuple.getValue1();
		}

		try {
			FileWriter writer = new FileWriter(outputPath, true);
			for (int numOfCluster = 2; numOfCluster <= maxNumCluster; numOfCluster++) {
				KMeansSample kmeansFullObject = new KMeansSample(numOfCluster, 200, fullClusterObject);
				ArrayList<ArrayList<Integer>> curClusters = kmeansFullObject.clustering(false, 0);
				this.allDataCenters = kmeansFullObject.getCluster();
				System.out.println("numOfCluster = " + numOfCluster);
				System.out.println("centers: ");
				for (int i=0; i<numOfCluster; i++) {
					System.out.println(Arrays.toString(allDataCenters[i]));
				}
				double[] variances = computeVariance(allDataCenters, curClusters, fullClusterObject);
				Arrays.sort(variances);
				System.out.println("variances = " +Arrays.toString(variances));
				String str = "";
				double avgVariance = 0.0;
				for (int i=variances.length-1; i>=0; i--) {
					str += variances[i] + ",";
					avgVariance += variances[i];
				}
				avgVariance = avgVariance / numOfCluster;
				str += avgVariance + "\n";
				writer.write(str);
				writer.close();
				System.out.println("avg variance = " + avgVariance);
				writer = new FileWriter(outputPath, true);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private double[] computeVariance(double[][] centers, ArrayList<ArrayList<Integer>> curClusters,
			double[][] data) {

		double[] variances = new double[centers.length];
		for (int cId = 0; cId < centers.length; cId++) {
			double variance = 0;
			double[] center = centers[cId];
			ArrayList<Integer> objectIds = curClusters.get(cId);
			int clusterSize = objectIds.size();
			for (int i=0; i<clusterSize; i++) {
				int oId = objectIds.get(i);
				variance += getDis(center, data[oId]);
			}
			variance = variance / clusterSize;
			variances[cId] = variance;
		}

		return variances;
	}

	private double getDis(double[] center, double[] cell) {
		return Math.sqrt(Math.pow((center[0] - cell[0]), 2) + Math.pow((center[1] - cell[1]), 2));

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/"
				+ "UCR/Research/Spatial/sketches/vldb2024/expData/point/";
		String filePath = "sample.csv";
		int maxNumCluster = 5;
		String outputPath = "sample.txt";

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-filePath")) {
				filePath = args[++i];
			} else if (args[i].equals("-outputPath")) {
				outputPath = args[++i];
			} else if (args[i].equals("-maxNumCluster")) {
				maxNumCluster = Integer.parseInt(args[++i]);
			}
		}

		filePath = folder + filePath;
		outputPath = folder + outputPath;
		ClusteringTestK run = new ClusteringTestK(folder, filePath, maxNumCluster, outputPath);
	}

}
