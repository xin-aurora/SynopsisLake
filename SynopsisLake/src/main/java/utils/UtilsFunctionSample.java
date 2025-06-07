package utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import dataStructure.RangeQuery1D;
import dataStructure.RangeQuery2D;

public class UtilsFunctionSample {
	public static double getDis(double[] center, double[] cell) {
		return Math.sqrt(Math.pow((center[0] - cell[0]), 2) + Math.pow((center[1] - cell[1]), 2));

	}

	public static double[] computeVariance(double[][] centers, ArrayList<ArrayList<Integer>> curClusters,
			double[][] data) {

		double[] variances = new double[centers.length];
		for (int cId = 0; cId < centers.length; cId++) {
			double variance = 0;
			double[] center = centers[cId];
			ArrayList<Integer> objectIds = curClusters.get(cId);
			int clusterSize = objectIds.size();
			for (int i = 0; i < clusterSize; i++) {
				int oId = objectIds.get(i);
				variance += getDis(center, data[oId]);
			}
			variance = variance / clusterSize;
			variances[cId] = variance;
		}

		return variances;
	}
	
	public static double[] computeVariance1D(double[] centers, ArrayList<ArrayList<Integer>> curClusters,
			double[][] data) {

		double[] variances = new double[centers.length];
		for (int cId = 0; cId < centers.length; cId++) {
			double variance = 0;
			double center = centers[cId];
			ArrayList<Integer> objectIds = curClusters.get(cId);
			int clusterSize = objectIds.size();
			for (int i = 0; i < clusterSize; i++) {
				int oId = objectIds.get(i);
				variance += Math.sqrt(Math.pow((center - data[oId][0]), 2));
			}
			variance = variance / clusterSize;
			variances[cId] = variance;
		}

		return variances;
	}

	public static double rangeQueryEst(RangeQuery2D query, double[][] dataList, double sampleRate) {
		double ans = 0.0;
		int cnt = 0;
		for (int i = 0; i < dataList.length; i++) {
			double[] data = dataList[i];
			// double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double lon,
			// double lat
			if (UtilsFunction.isOverlapPointCell(query.minLon, query.minLat, query.maxLon, query.maxLat, data[0],
					data[1])) {
				cnt++;
			}
		}
		ans = cnt * sampleRate;
		return ans;
	}

	public static double rangeQueryEst(RangeQuery2D query, double[][] dataList, int queryIdx, double sampleRate) {
		double ans = 0.0;
		int cnt = 0;
		for (int i = 0; i < dataList.length; i++) {
			double[] data = dataList[i];
			// double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double lon,
			// double lat
			if (UtilsFunction.isOverlapPointCell(query.minLon, query.minLat, query.maxLon, query.maxLat, data[0],
					data[1])) {
				cnt += data[queryIdx];
			}
		}
		ans = cnt * sampleRate;
		return ans;
	}
	
	public static double rangeQuery1DEst(RangeQuery1D query, double[][] dataList, double sampleRate) {
		double ans = 0.0;
		int cnt = 0;
		for (int i = 0; i < dataList.length; i++) {
			double[] data = dataList[i];
			// double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double lon,
			// double lat
			if (UtilsFunction.isOverlapPointInterval(query.minLon, query.maxLon, data[0])) {
				cnt++;
			}
		}
		ans = cnt * sampleRate;
		return ans;
	}
	
	public static double rangeQuery1DEst(RangeQuery1D query, double[][] dataList, int queryIdx, double sampleRate) {
		double ans = 0.0;
		int cnt = 0;
		for (int i = 0; i < dataList.length; i++) {
			double[] data = dataList[i];
			// double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double lon,
			// double lat
			if (UtilsFunction.isOverlapPointInterval(query.minLon, query.maxLon, data[0])) {
				cnt += data[queryIdx];
			}
		}
		ans = cnt * sampleRate;
		System.out.println(ans);
		return ans;
	}
}
