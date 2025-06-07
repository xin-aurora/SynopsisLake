package operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import utils.WeightedRandomPick;

public class KMeansSample {

	private double[][] centers;
	private int numCluster;
	private int numMaxIts;
	private double[][] data;
	private boolean[] pickPos;

	double curTotalError = 0.0;

	public KMeansSample(int numCluster, int numMaxIts, double[][] data) {
		this.centers = new double[numCluster][2];
		this.numCluster = numCluster;
		pickPos = new boolean[data.length];
		this.numMaxIts = numMaxIts;
		this.data = data;
	}
	
	public double[][] getCluster() {
		return this.centers;
	}
	
	public ArrayList<ArrayList<Integer>> clustering(boolean computerConfidenceInterval, double delta) {

		initialCenters();

		int curRound = 0;
		ArrayList<ArrayList<Integer>> clusters = computeError(centers);
		double preTotalError = curTotalError;
		while (curRound < numMaxIts) {
//			System.out.println("round = " + curRound);
			double[][] updatedCenters = updateCenters(curRound, clusters);
			ArrayList<ArrayList<Integer>> curClusters = computeError(updatedCenters);
//			totalClusters.add(curClusters);
			if (Math.abs(preTotalError - curTotalError) < 1.1102230246251565E-10) {
				clusters = curClusters;
				break;
			} else {
				centers = updatedCenters;
				preTotalError = curTotalError;
				clusters = curClusters;
			}
			curRound++;
		}

//		System.out.println("training round = " + curRound);
		return clusters;
	}

	private ArrayList<double[][]> computerConfidenceInterval(double delta, ArrayList<ArrayList<Integer>> clusters) {
		ArrayList<double[][]> confidenceInterval = new ArrayList<double[][]>();

		// E(mean) = Sample mean +- (Sample variance * \sqrt( 2log(3/\delta) / t))
		// + 3Rlog(3/\delta) / t
		for (int cId = 0; cId < clusters.size(); cId++) {
			ArrayList<Integer> objectIds = clusters.get(cId);
			double lonTotalDiffPow = 0.0;
			double latTotalDiffPow = 0.0;
			for (int oId = 0; oId < objectIds.size(); oId++) {
				double[] object = data[oId];
				lonTotalDiffPow += Math.pow(object[0] - centers[cId][0], 2);
				latTotalDiffPow += Math.pow(object[1] - centers[cId][1], 2);
			}
			double lonVariance = lonTotalDiffPow / objectIds.size();
			double latVariance = latTotalDiffPow / objectIds.size();
		}

		return confidenceInterval;
	}

	private double[] confidenceInterval(double delta, double sampleMean, double sampleVariance, double R, int t) {

		double[] confidenceInterval = new double[2];
		// E(mean) = Sample mean +- (Sample variance * \sqrt( 2log(3/\delta) / t))
		// + 3Rlog(3/\delta) / t
		
		
		return confidenceInterval;
	}

	private double[][] updateCenters(int round, ArrayList<ArrayList<Integer>> cluters) {

		double[][] updatedCenters = new double[numCluster][2];
		for (int cId = 0; cId < numCluster; cId++) {
			ArrayList<Integer> cluster = cluters.get(cId);
			double[] newCenter = new double[2];
			double newLon = 0;
			double newLat = 0;
			double totalCount = 0;
			for (int i = 0; i < cluster.size(); i++) {
//				SSH2DCell curCell = cells[cluster.get(i)[0]][cluster.get(i)[1]];
				int idx = cluster.get(i);
				double[] point = data[idx];
				newLon += point[0];
				newLat += point[1];
				totalCount++;
			}
			if (totalCount == 0) {

				System.out.println("cluster = " + cId + ", contains " + cluster.size() + " cells");

				// debug
//				debugRepick(round);
				newCenter = pickNewOneCenters();
//				System.out.println("new pick center = " + Arrays.toString(newCenter));
				updatedCenters[cId] = newCenter;
				System.out.println("pick a new cluster");
			} else {
				newCenter[0] = newLon / totalCount;
				newCenter[1] = newLat / totalCount;
				updatedCenters[cId] = newCenter;
			}
		}
		return updatedCenters;
	}

	private ArrayList<ArrayList<Integer>> computeError(double[][] centerPoints) {

		curTotalError = 0.0;

		ArrayList<ArrayList<Integer>> clusters = new ArrayList<ArrayList<Integer>>();
		for (int i = 0; i < numCluster; i++) {
			clusters.add(new ArrayList<Integer>());
		}
		for (int pId = 0; pId < data.length; pId++) {
			double[] curPoint = data[pId];
			double minScore = Double.MAX_VALUE;
			int belongCluster = -1;
			for (int cId = 0; cId < centers.length; cId++) {
//					double score = f * getDis(centerPoints[cId], curCenter);
				double score = getDis(centerPoints[cId], curPoint);
				if (score < minScore) {
					minScore = score;
					belongCluster = cId;
				}
			}
			clusters.get(belongCluster).add(pId);
			curTotalError += minScore;

		}

		return clusters;
	}

	private void initialCenters() {
		int idx = 0;
		// picl first center
		Random rand = new Random();
		int firstCenterIdx = rand.nextInt(data.length) + 1;
		pickPos[firstCenterIdx] = true;
		double[] center = data[firstCenterIdx];
		centers[idx] = center;
		idx++;
		pickNextCenters(idx);
	}

	private void pickNextCenters(int idx) {

		WeightedRandomPick<Integer> weightPick = new WeightedRandomPick<Integer>();

		for (int i = 0; i < data.length; i++) {
			if (!pickPos[i]) {
				double[] point = data[i];
				double maxCurScore = Double.MIN_VALUE;
				for (int cId = 0; cId < centers.length; cId++) {
					double[] c = centers[cId];
					double score = getDis(c, point);
					maxCurScore = Math.max(score, maxCurScore);
				}
				if (maxCurScore > Double.MIN_VALUE) {
					weightPick.add(maxCurScore, i);
				}

			}

		}

		while (idx < numCluster) {
			int pick = weightPick.next();
			pickPos[pick] = true;
//				initCentersPos[idx] = pick;
			double[] newCenter = data[pick];
			centers[idx] = newCenter;
			idx++;
		}

	}

	private double[] pickNewOneCenters() {

		WeightedRandomPick<Integer> weightPick = new WeightedRandomPick<Integer>();

		for (int i = 0; i < data.length; i++) {
			if (!pickPos[i]) {
				double[] point = data[i];
				double maxCurScore = Double.MIN_VALUE;
				for (int cId = 0; cId < centers.length; cId++) {
					double[] c = centers[cId];
					double score = getDis(c, point);
					maxCurScore = Math.max(score, maxCurScore);
				}
				if (maxCurScore > Double.MIN_VALUE) {
					weightPick.add(maxCurScore, i);
				}

			}

		}

		int pick = weightPick.next();
		pickPos[pick] = true;
//				initCentersPos[idx] = pick;
		double[] newCenter = data[pick];

		return newCenter;

	}

	private double getDis(double[] center, double[] cell) {
		return Math.sqrt(Math.pow((center[0] - cell[0]), 2) + Math.pow((center[1] - cell[1]), 2));

	}
}
