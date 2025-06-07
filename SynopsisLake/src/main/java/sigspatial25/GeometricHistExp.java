package sigspatial25;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import baseline.NearOptimalPartitioning;
import histogram.GeometricHistogramOpt;
import histogram.SimpleSpatialHistogramOpt;
import operator.BetaRatioMetricDensityDiff1DParNorm;
import operator.SplitUnitRangePartitioning;
import utils.UtilsFunctionHistogram;

public class GeometricHistExp {

	double MinMinLon = Double.MAX_VALUE;
	double MaxMaxLon = -Double.MAX_VALUE;
	double MinMinLat = Double.MAX_VALUE;
	double MaxMaxLat = -Double.MAX_VALUE;
	String folder;
//	int numOfFile = 4;
	int[] mergeResolutions = { 64, 128, 258 };
	int mergeResolution;
	int lonTargetResolution;
	int latTargetResolution;
	String srcHistPath;
//	int[] srcResolution = { 10, 16, 8, 12 };
	int[] srcResolutions = { 64, 128, 256 };
	int srcResolution;
	int numOfFile = 6;

	double varepsilon = 1e-6;
	int scoreFunction = 2;
	double weightFactor = 0.5;

	boolean joinQuery = true;

	double scaleFactor = 0.5;

	public GeometricHistExp(String folder) {
		this.folder = folder;
		for (int i = 0; i < srcResolutions.length; i++) {
			this.srcResolution = srcResolutions[i];
			this.mergeResolution = mergeResolutions[i];
			System.out.println("----Resolution " + this.srcResolution + "----");
			mergedPrecisionExp();
			System.out.println();
		}
	}

	private void mergedPrecisionExp() {

		GeometricHistogramOpt[] srcHists = loadSynopsis();

//		setAlignParameters(2, 0.5);
		this.lonTargetResolution = mergeResolutions[0];
		this.latTargetResolution = mergeResolutions[1];
		System.out.println("---Objective Function---");
		long duration = 0;
		long startObjTime = System.nanoTime();
		ArrayList<double[]> alignCombineSolutions = dataDrivenAlignSolution(srcHists);
		GeometricHistogramOpt[] alignCombineHists = targetHistograms(alignCombineSolutions.get(0),
				alignCombineSolutions.get(1));
		long endObjTime = System.nanoTime();
		duration += (endObjTime - startObjTime);
		System.out.println("Aggregation");
		long mergeStart = System.nanoTime();
		for (int tId = 0; tId < alignCombineHists.length; tId++) {
			for (int sId = 0; sId < srcHists.length; sId++) {
//				System.out.println(srcHists[sId].totalN);
				alignCombineHists[tId].aggregateHistogramSep(srcHists[sId]);
			}
		}
		long mergeEnd = System.nanoTime();
		duration += (mergeEnd - mergeStart);

		System.out.println("---Uniform Solution---");
		// uniform merge
		GeometricHistogramOpt uniformHist = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
				mergeResolutions[0], mergeResolutions[1]);
		System.out.println("Aggregation");
		for (int sId = 0; sId < srcHists.length; sId++) {
			uniformHist.aggregateHistogramSep(srcHists[sId]);
		}

		// original data
		System.out.println("Start with loading original data");
		ArrayList<double[]> dataList = loadOriginalData(numOfFile);
		System.out.println("total data size = " + dataList.size());
		System.out.println("Done with loading original data");

		System.out.println("num of target hist = " + alignCombineHists.length);
		// evaluation
		GeometricHistogramOpt[] alignHistGTs = targetHistograms(alignCombineSolutions.get(0),
				alignCombineSolutions.get(1));
		for (int tId = 0; tId < alignHistGTs.length; tId++) {
			alignHistGTs[tId].loadHistByDataArray(alignHistGTs[tId], dataList);
		}

		System.out.println("align evaluation");
		UtilsFunctionHistogram.GeometricHistogramOptEvaluation(alignCombineHists, alignHistGTs);

		GeometricHistogramOpt uniformHistGT = new GeometricHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
				mergeResolutions[0], mergeResolutions[1]);
		uniformHistGT.loadHistByDataArray(uniformHistGT, dataList);
		System.out.println("uniform evaluation");
		UtilsFunctionHistogram.GHOptEvaluation(uniformHist, uniformHistGT);

		if (joinQuery) {
			long joinStart = System.nanoTime();
			for (int round = 0; round < 10000; round++) {
				for (int i = 0; i < alignCombineHists.length; i++) {
					alignCombineHists[i].selectivityEstimation(alignCombineHists[i]);
				}
			}
			long joinEnd = System.nanoTime();
			long queryTime = (joinEnd - joinStart) / 10000;
//			duration += queryTime;
			uniformHistGT.selectivityEstimation(uniformHistGT);

			System.out.println("combine time = " + duration * 1E-6 + " ms.");
			System.out.println("query time = " + queryTime * 1E-6 + " ms.");
		}

	}

	private GeometricHistogramOpt[] loadSynopsis() {
		GeometricHistogramOpt[] hists = new GeometricHistogramOpt[numOfFile];
//		System.out.println("numOfFile = " + numOfFile);
		for (int fileId = 0; fileId < numOfFile; fileId++) {
			String inputPath = folder + fileId + ".csv";
//			System.out.println(inputPath);
			double minLonSingle = Double.MAX_VALUE;
			double maxLonSingle = -Double.MAX_VALUE;
			double minLatSingle = Double.MAX_VALUE;
			double maxLatSingle = -Double.MAX_VALUE;
			ArrayList<double[]> dataList = new ArrayList<double[]>();
			BufferedReader dataReader;
			try {
				dataReader = new BufferedReader(new FileReader(inputPath));
				String dataStr = dataReader.readLine();
				while (dataStr != null) {
					String[] tmp = dataStr.split(";");
					if (tmp.length > 4) {
						double minLon = Double.parseDouble(tmp[0]);
						double minLat = Double.parseDouble(tmp[1]);
						double maxLon = Double.parseDouble(tmp[2]);
						double maxLat = Double.parseDouble(tmp[3]);
						double[] data = new double[4];
						data[0] = minLon;
						data[1] = minLat;
						data[2] = maxLon;
						data[3] = maxLat;
						dataList.add(data);
						MinMinLon = Math.min(MinMinLon, minLon);
						MaxMaxLon = Math.max(MaxMaxLon, maxLon);
						MinMinLat = Math.min(MinMinLat, minLat);
						MaxMaxLat = Math.max(MaxMaxLat, maxLat);
						minLonSingle = Math.min(minLonSingle, minLon);
						maxLonSingle = Math.max(maxLonSingle, maxLon);
						minLatSingle = Math.min(minLatSingle, minLat);
						maxLatSingle = Math.max(maxLatSingle, maxLat);
					}
					dataStr = dataReader.readLine();
				}
				dataReader.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			minLonSingle = minLonSingle - varepsilon;
			maxLonSingle = maxLonSingle + varepsilon;
			minLatSingle = minLatSingle - varepsilon;
			maxLatSingle = maxLatSingle + varepsilon;
//			System.out.println("data size = " + dataList.size());
//			System.out.println(minLonSingle + " - " + maxLonSingle);
//			System.out.println(minLatSingle + " - " + maxLatSingle);
			GeometricHistogramOpt hist = new GeometricHistogramOpt(minLonSingle, minLatSingle, maxLonSingle,
					maxLatSingle, srcResolution, srcResolution);
			for (int dId = 0; dId < dataList.size(); dId++) {
				double[] data = dataList.get(dId);
//				System.out.println(hist.totalN);
				hist.addRecord(data[0], data[1], data[2], data[3]);
			}
			hists[fileId] = hist;
//			System.out.println(hist.totalN);
		}
		MinMinLon = MinMinLon - varepsilon;
		MaxMaxLon = MaxMaxLon + varepsilon;
		MinMinLat = MinMinLat - varepsilon;
		MaxMaxLat = MaxMaxLat + varepsilon;
//		System.out.println("MinMinLon = " + MinMinLon + ", MinMinLat = " + MinMinLat);
//		System.out.println("MaxMaxLon = " + MaxMaxLon + ", MaxMaxLat = " + MaxMaxLat);

		return hists;
	}

	protected ArrayList<double[]> dataDrivenAlignSolution(GeometricHistogramOpt[] srcHists) {
		ArrayList<double[]> lonSrcs = new ArrayList<double[]>();
		ArrayList<double[]> latSrcs = new ArrayList<double[]>();
		int totalNumLonBoundaries = 0;
		int totalNumLatBoundaries = 0;
		ArrayList<double[]> lonDatas = new ArrayList<double[]>();
		ArrayList<double[]> latDatas = new ArrayList<double[]>();
		for (int i = 0; i < srcHists.length; i++) {
			double[] lon = new double[2];
			lon[0] = srcHists[i].getMinLon();
			lon[1] = srcHists[i].getMaxLon();
			lonSrcs.add(lon);
			totalNumLonBoundaries += lon.length;
			double[] lat = new double[2];
			lat[0] = srcHists[i].getMinLat();
			lat[1] = srcHists[i].getMaxLat();
			latSrcs.add(lat);
			totalNumLatBoundaries += lat.length;
//			double[][] data = srcHists[i].getData();
			double[] lonData = new double[lon.length - 1];
			double[] latData = new double[lat.length - 1];
			lonData[0] = srcHists[i].totalN;
			latData[0] = srcHists[i].totalN;
			lonDatas.add(lonData);
			latDatas.add(latData);
//			System.out.println(Arrays.toString(lon));
//			System.out.println(Arrays.toString(lat));
//			System.out.println(Arrays.toString(lonData));
//			System.out.println(Arrays.toString(latData));
//			System.out.println();
		}

		int size = 5;
		BetaRatioMetricDensityDiff1DParNorm alignCombineLon = new BetaRatioMetricDensityDiff1DParNorm(size,
				scoreFunction, MinMinLon);
		alignCombineLon.setWeightFactor(weightFactor);
		double[] alignCombineLonSolution = alignCombineLon.reshaping(totalNumLonBoundaries, lonSrcs, lonDatas,
				scaleFactor, varepsilon);
		BetaRatioMetricDensityDiff1DParNorm alignCombineLat = new BetaRatioMetricDensityDiff1DParNorm(size,
				scoreFunction, MinMinLat);
		alignCombineLat.setWeightFactor(weightFactor);
		double[] alignCombineLatSolution = alignCombineLat.reshaping(totalNumLatBoundaries, latSrcs, latDatas,
				scaleFactor, varepsilon);
		ArrayList<double[]> alignCombineSolutions = new ArrayList<double[]>();
		alignCombineSolutions.add(alignCombineLonSolution);
		alignCombineSolutions.add(alignCombineLatSolution);
//		System.out.println("AlignFunction lon bound size: " + alignCombineLonSolution.length + ", lat bound size: "
//				+ alignCombineLatSolution.length);
//		System.out.println(Arrays.toString(alignCombineLonSolution));
//		System.out.println(Arrays.toString(alignCombineLatSolution));
		return alignCombineSolutions;
	}

	private ArrayList<double[]> loadOriginalData(int numFile) {
		ArrayList<double[]> dataList = new ArrayList<double[]>();

		for (int fileId = 0; fileId < numFile; fileId++) {
			String filePath = folder + fileId + ".csv";
			File file = new File(filePath);

			try {
				BufferedReader reader = new BufferedReader(new FileReader(file));

//				String header = reader.readLine();
				String line = reader.readLine();

				while (line != null) {
					String[] tmp = line.split(";");
					if (tmp.length > 4) {
						double lonMin = Double.parseDouble(tmp[0]);
						double lonMax = Double.parseDouble(tmp[2]);

						double latMin = Double.parseDouble(tmp[1]);
						double latMax = Double.parseDouble(tmp[3]);

						double[] data = new double[4];
						data[0] = lonMin;
						data[1] = latMin;
						data[2] = lonMax;
						data[3] = latMax;
						dataList.add(data);
					}
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
		}

		return dataList;

	}

	private GeometricHistogramOpt[] targetHistograms(double[] lonBounds, double[] latBounds) {
		int numLon = lonBounds.length - 1;
		int numLat = latBounds.length - 1;
		GeometricHistogramOpt[] histograms = new GeometricHistogramOpt[numLon * numLat];
		int histId = 0;
		int lonBucket = mergeResolution;
		int latBucket = mergeResolution;
		double minLon = lonBounds[0];
		for (int i = 1; i <= numLon; i++) {
			double maxLon = lonBounds[i];
//			int lonBucket = (int) (mergeResolutions[0] * (maxLon-minLon) / (MaxMaxLon - MinMinLon));
			double minLat = latBounds[0];
			for (int j = 1; j <= numLat; j++) {
				double maxLat = latBounds[j];
//				int latBucket = (int) (mergeResolutions[1] * (maxLat-minLat) / (MaxMaxLat - MinMinLat));
				GeometricHistogramOpt hist = new GeometricHistogramOpt(minLon, minLat, maxLon, maxLat, lonBucket,
						latBucket);
//				System.out.println(minLon + " - " + maxLon);
//				System.out.println(minLat + " - " + maxLat);
				histograms[histId] = hist;
				histId++;
				minLat = maxLat;
			}
			minLon = maxLon;
		}

		return histograms;
	}

	public static void main(String[] args) {
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/"
				+ "sketches/sigspatial/exp/sj/park6Par/";
//		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/"
//				+ "sketches/sigspatial/exp/sj/park4Par/";
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			}
		}
		GeometricHistExp run = new GeometricHistExp(folder);
	}

}
