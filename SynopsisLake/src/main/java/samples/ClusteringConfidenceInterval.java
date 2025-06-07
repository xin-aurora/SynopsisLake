package samples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import dataStructure.Tuple2;
import operator.KMeansSample;

public class ClusteringConfidenceInterval {

	double[][] sampleCenters = null;
	double[][] allDataCenters = null;

	public ClusteringConfidenceInterval(String folder, String filePath, int numOfCluster, double sampleRate,
			double confidenceInterval) {

		ArrayList<Tuple2<Double, Double>> dataList = loadFile(filePath);
		UniformSamples<Tuple2<Double, Double>> uniformSamplesHelper = new UniformSamples<Tuple2<Double, Double>>(
				dataList.size(), sampleRate);
		List<Tuple2<Double, Double>> samples = 	uniformSamplesHelper.Sample(dataList);

//		System.out.println("sample size = " + samples.size());
		runKMeans(dataList, samples, confidenceInterval);
	}

	private void runKMeans(ArrayList<Tuple2<Double, Double>> dataList, List<Tuple2<Double, Double>> samples,
			double confidenceInterval) {
		double[][] sampleClusterObject = new double[samples.size()][2];

		double[][] fullClusterObject = new double[dataList.size()][2];
		for (int i = 0; i < dataList.size(); i++) {
			Tuple2<Double, Double> tuple = dataList.get(i);
			fullClusterObject[i][0] = tuple.getValue0();
			fullClusterObject[i][1] = tuple.getValue1();
		}

		for (int i = 0; i < samples.size(); i++) {
			Tuple2<Double, Double> tuple = samples.get(i);
			sampleClusterObject[i][0] = tuple.getValue0();
			sampleClusterObject[i][1] = tuple.getValue1();
		}

		KMeansSample kmeansSample = new KMeansSample(15, 200, sampleClusterObject);
		double delta = 1.0 - confidenceInterval;
		kmeansSample.clustering(true, delta);
		this.sampleCenters = kmeansSample.getCluster();

		KMeansSample kmeansFullObject = new KMeansSample(15, 200, fullClusterObject);
		kmeansFullObject.clustering(false, 0);
		this.allDataCenters = kmeansFullObject.getCluster();

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

	}

}
