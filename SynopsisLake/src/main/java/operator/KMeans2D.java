package operator;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import utils.WeightedRandomPick;

public class KMeans2D {
	private double[][] centers;
	private int numCluster;
	private int numMaxIts;
	private double[][] data;
	private double[] lonBoundary;
	private double[] latBoundary;
	private int numLonBucket;
	private int numLatBucket;

	private boolean[][] pickPos;

	double curTotalError = 0.0;

	// for debug
//	ArrayList<ArrayList<ArrayList<int[]>>> totalClusters;
	String outputPath;

	public KMeans2D(int numCluster, int numMaxIts, double[][] data, double[] lonBoundary, double[] latBoundary,
			String outputPath) {
		this.centers = new double[numCluster][2];
		this.numCluster = numCluster;
		this.numLonBucket = lonBoundary.length-1;
		this.numLatBucket = latBoundary.length-1;
		pickPos = new boolean[numLonBucket][numLatBucket];
		this.numMaxIts = numMaxIts;
		this.data = data;
		this.lonBoundary = lonBoundary;
		this.latBoundary = latBoundary;
//		totalClusters = new ArrayList<ArrayList<ArrayList<int[]>>>();
		this.outputPath = outputPath;
	}

	public double[][] clustering(boolean writeLastRound) {

		initialCenters();

		int curRound = 0;
		ArrayList<ArrayList<int[]>> clusters = computeError(centers);
//		totalClusters.add(clusters);
//		debugRepick(curRound);
		double preTotalError = curTotalError;
		while (curRound < numMaxIts) {
//			System.out.println("round = " + curRound);
			double[][] updatedCenters = updateCenters(curRound, clusters);
			ArrayList<ArrayList<int[]>> curClusters = computeError(updatedCenters);
//			totalClusters.add(curClusters);
			if (Math.abs(preTotalError - curTotalError) < 1.1102230246251565E-10) {
				break;
			} else {
				centers = updatedCenters;
				preTotalError = curTotalError;
				clusters = curClusters;
			}
			curRound++;
		}

		return centers;
	}

	private double[][] updateCenters(int round, ArrayList<ArrayList<int[]>> cluters) {

		double[][] updatedCenters = new double[numCluster][2];
		for (int cId = 0; cId < numCluster; cId++) {
			ArrayList<int[]> cluster = cluters.get(cId);
			double[] newCenter = new double[2];
			double newLon = 0;
			double newLat = 0;
			double totalCount = 0;
			for (int i = 0; i < cluster.size(); i++) {
//				SSH2DCell curCell = cells[cluster.get(i)[0]][cluster.get(i)[1]];
				int lonIdx = cluster.get(i)[0];
				int latIdx = cluster.get(i)[1];
				double val = data[lonIdx][latIdx];
				double[] curCenter = getCellCenter(lonBoundary[lonIdx], lonBoundary[lonIdx+1], 
						latBoundary[latIdx], latBoundary[latIdx+1]);
				newLon += curCenter[0] * val;
				newLat += curCenter[1] * val;
				totalCount += val;
			}
			if (totalCount == 0) {

				System.out.println("cluster = " + cId + ", contains " + cluster.size() + " cells");

				// debug
//				debugRepick(round);
				newCenter = pickNextCenters(cId);
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

	private ArrayList<ArrayList<int[]>> computeError(double[][] centerPoints) {

		curTotalError = 0.0;

		ArrayList<ArrayList<int[]>> clusters = new ArrayList<ArrayList<int[]>>();
		for (int i = 0; i < numCluster; i++) {
			clusters.add(new ArrayList<int[]>());
		}
		for (int lon = 0; lon < numLonBucket; lon++) {
			for (int lat = 0; lat < numLatBucket; lat++) {
				int[] curPos = new int[2];
				curPos[0] = lon;
				curPos[1] = lat;
				double[] curCenter = getCellCenter(lonBoundary[lon], lonBoundary[lon+1], latBoundary[lat],
						latBoundary[lat+1]);
//				double f = cells[lon][lat].getVal();
				double minScore = Double.MAX_VALUE;
				int belongCluster = -1;
				for (int cId = 0; cId < centers.length; cId++) {
//					double score = f * getDis(centerPoints[cId], curCenter);
					double score = getDis(centerPoints[cId], curCenter);
					if (score < minScore) {
						minScore = score;
						belongCluster = cId;
					}
				}
				clusters.get(belongCluster).add(curPos);
				curTotalError += minScore;

			}
		}
		return clusters;
	}

	private void initialCenters() {
		int idx = 0;
		double[] center = pickFirstCenters();
		centers[idx] = center;
		idx++;
		while (idx < numCluster) {
			double[] newCenter = pickNextCenters(idx);
			centers[idx] = newCenter;
			idx++;
		}
	}

	private double[] pickFirstCenters() {

		WeightedRandomPick<int[]> weightPick = new WeightedRandomPick<int[]>();

		for (int lon = 0; lon < numLonBucket; lon++) {
			for (int lat = 0; lat < numLatBucket; lat++) {
				int[] pos = new int[2];
				pos[0] = lon;
				pos[1] = lat;
				double score = data[lon][lat];
				weightPick.add(score, pos);
			}

		}

		int[] pick = weightPick.next();
		pickPos[pick[0]][pick[1]] = true;
//		initCentersPos[0] = pick;
//		System.out.println(Arrays.toString(pick));
		double[] center = getCellCenter(lonBoundary[pick[0]], lonBoundary[pick[0]+1], 
				latBoundary[pick[1]], latBoundary[pick[1]+1]);

		return center;
	}

	private double[] pickNextCenters(int idx) {

		WeightedRandomPick<int[]> weightPick = new WeightedRandomPick<int[]>();
		for (int lon = 0; lon < numLonBucket; lon++) {
			for (int lat = 0; lat < numLatBucket; lat++) {
				if (!pickPos[lon][lat]) {
					int[] pos = new int[2];
					pos[0] = lon;
					pos[1] = lat;
					double[] curCenter = getCellCenter(lonBoundary[lon], lonBoundary[lon+1], 
							latBoundary[lat], latBoundary[lat+1]);
					double f = data[lon][lat];
					double maxCurScore = Double.MIN_VALUE;
					for (int cId = 0; cId < centers.length; cId++) {
						double[] c = centers[cId];
						double score = f * getDis(c, curCenter);
						maxCurScore = Math.max(score, maxCurScore);
					}
					if (maxCurScore > Double.MIN_VALUE) {
						weightPick.add(maxCurScore, pos);
					}

				}
			}
		}

		int[] pick = weightPick.next();
		pickPos[pick[0]][pick[1]] = true;
//		initCentersPos[idx] = pick;
		double[] center = getCellCenter(lonBoundary[pick[0]], lonBoundary[pick[0]+1], 
				latBoundary[pick[1]], latBoundary[pick[1]+1]);
		return center;
	}

	private double[] getCellCenter(double cellMinLon, double cellMaxLon, double cellMinLat, double cellMaxLat) {
		double[] center = new double[2];
		center[0] = (cellMaxLon - cellMinLon) / 2 + cellMinLon;
		center[1] = (cellMaxLat - cellMinLat) / 2 + cellMinLat;
		return center;
	}

	private double getDis(double[] center, double[] cell) {
		return Math.sqrt(Math.pow((center[0] - cell[0]), 2) + Math.pow((center[1] - cell[1]), 2));

	}

//	private void debugRepick(int round) {
////		String filePath = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/data/medium/kmeans/";
//
////		for (int r = round-1; r<= round; r++) {
//			ArrayList<ArrayList<int[]>> clusters = totalClusters.get(round);
//			for (int cId = 0; cId < numCluster; cId++) {
//				String path = outputPath + "r" + round + "_" + cId + ".csv";
//				try {
//					FileWriter myWriter = new FileWriter(path);
//					ArrayList<int[]> cluster = clusters.get(cId);
//					String str = "lon:" + centers[cId][0] + ",lat:" + centers[cId][1] + ",val" + "\n";
//					myWriter.write(str);
//					str = centers[cId][0] + "," + centers[cId][1] + ",--" + "\n";
//					myWriter.write(str);
//					for (int i = 0; i < cluster.size(); i++) {
//						SSH2DCell curCell = cells[cluster.get(i)[0]][cluster.get(i)[1]];
//						double[] curCenter = getCellCenter(curCell);
//						str = curCenter[0] + "," + curCenter[1] + "," + curCell.getVal() + "\n";
//						myWriter.write(str);
//					}
//					myWriter.close();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//
//			}
////		}
//	}

}
