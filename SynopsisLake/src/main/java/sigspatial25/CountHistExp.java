package sigspatial25;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.math3.distribution.BetaDistribution;

import baseline.NearOptimal1D;
import histogram.SimpleSpatialHistogramOpt;
import operator.BetaRatioMetricDensityDiff1DParNorm;
import operator.QueryDriven1DPar;
import utils.UtilsFunctionHistogram;

public class CountHistExp {

	double MinMinLon = Double.MAX_VALUE;
	double MaxMaxLon = -Double.MAX_VALUE;
	double MinMinLat = Double.MAX_VALUE;
	double MaxMaxLat = -Double.MAX_VALUE;
	String folder;
	int numOfFile;
	int[] mergeResolutions;
	int lonTargetResolution;
	int latTargetResolution;
	String srcHistPath;
	int[] srcResolution;

	// alignMerge, parameters
	int scoreFunction;
	double varepsilon = 1e-6;
	// combine align and density difference
	double scaleFactor = 0.5;
	double weightFactor;
	boolean doNorm = true;

	// range query
	double mergeRate;
	String rangeQueryPath;
	FileWriter queryErrorWriter;
	FileWriter histErrorWriter;

	public CountHistExp(String folder, String srcHistPath, int numOfFile, int scoreFunction, double weightFactor,
			String rangeQueryPath) {
		this.folder = folder;
		this.srcHistPath = srcHistPath;
		this.numOfFile = numOfFile;
		this.scoreFunction = scoreFunction;
		this.weightFactor = weightFactor;
		this.rangeQueryPath = rangeQueryPath;
	}
	
	public CountHistExp(String folder, String srcHistPath, int numOfFile,
			String rangeQueryPath) {
		this.folder = folder;
		this.srcHistPath = srcHistPath;
		this.numOfFile = numOfFile;
		this.rangeQueryPath = rangeQueryPath;
	}
	
	public CountHistExp() {
		
	}
	
	protected void setAlignParameters(int scoreFunction, double weightFactor) {
		this.scoreFunction = scoreFunction;
		this.weightFactor = weightFactor;
	}

	protected void setSrcResolution(int[] srcResolution) {
		this.srcResolution = srcResolution;
	}

	protected ArrayList<double[]> dataDrivenAlignSolution(SimpleSpatialHistogramOpt[] srcHists) {
		ArrayList<double[]> lonSrcs = new ArrayList<double[]>();
		ArrayList<double[]> latSrcs = new ArrayList<double[]>();
		int totalNumLonBoundaries = 0;
		int totalNumLatBoundaries = 0;
		ArrayList<double[]> lonDatas = new ArrayList<double[]>();
		ArrayList<double[]> latDatas = new ArrayList<double[]>();
		for (int i = 0; i < srcHists.length; i++) {
			double[] lon = srcHists[i].getLonBoundary();
			lonSrcs.add(lon);
			totalNumLonBoundaries += lon.length;
			double[] lat = srcHists[i].getLatBoundary();
			latSrcs.add(lat);
			totalNumLatBoundaries += lat.length;
			double[][] data = srcHists[i].getData();
			double[] lonData = new double[lon.length - 1];
			double[] latData = new double[lat.length - 1];
			for (int lonId = 0; lonId < data.length; lonId++) {
				for (int latId = 0; latId < data[0].length; latId++) {
					lonData[lonId] += data[lonId][latId];
					latData[latId] += data[lonId][latId];
				}
			}
			lonDatas.add(lonData);
			latDatas.add(latData);
		}

		BetaRatioMetricDensityDiff1DParNorm alignCombineLon = new BetaRatioMetricDensityDiff1DParNorm(
				lonTargetResolution, scoreFunction, MinMinLon);
		alignCombineLon.setWeightFactor(weightFactor);
		double[] alignCombineLonSolution = alignCombineLon.reshaping(totalNumLonBoundaries, lonSrcs, lonDatas,
				scaleFactor, varepsilon);
		BetaRatioMetricDensityDiff1DParNorm alignCombineLat = new BetaRatioMetricDensityDiff1DParNorm(
				latTargetResolution, scoreFunction, MinMinLat);
		alignCombineLat.setWeightFactor(weightFactor);
		double[] alignCombineLatSolution = alignCombineLat.reshaping(totalNumLatBoundaries, latSrcs, latDatas,
				scaleFactor, varepsilon);
		ArrayList<double[]> alignCombineSolutions = new ArrayList<double[]>();
		alignCombineSolutions.add(alignCombineLonSolution);
		alignCombineSolutions.add(alignCombineLatSolution);
//		System.out.println("AlignFunction lon bound size: " + alignCombineLonSolution.length + ", lat bound size: "
//				+ alignCombineLatSolution.length);
		return alignCombineSolutions;
	}

	protected ArrayList<double[]> queryDrivenAlignSolution(SimpleSpatialHistogramOpt[] srcHists,
			String inputQueryPath) {
		ArrayList<double[]> lonSrcs = new ArrayList<double[]>();
		ArrayList<double[]> latSrcs = new ArrayList<double[]>();
		int totalNumLonBoundaries = 0;
		int totalNumLatBoundaries = 0;
		ArrayList<double[]> lonDatas = new ArrayList<double[]>();
		ArrayList<double[]> latDatas = new ArrayList<double[]>();
		for (int i = 0; i < srcHists.length; i++) {
			double[] lon = srcHists[i].getLonBoundary();
			lonSrcs.add(lon);
			totalNumLonBoundaries += lon.length;
			double[] lat = srcHists[i].getLatBoundary();
			latSrcs.add(lat);
			totalNumLatBoundaries += lat.length;
			double[][] data = srcHists[i].getData();
			double[] lonData = new double[lon.length - 1];
			double[] latData = new double[lat.length - 1];
			for (int lonId = 0; lonId < data.length; lonId++) {
				for (int latId = 0; latId < data[0].length; latId++) {
					lonData[lonId] += data[lonId][latId];
					latData[latId] += data[lonId][latId];
				}
			}
			lonDatas.add(lonData);
			latDatas.add(latData);
		}

		QueryDriven1DPar alignCombineLon = new QueryDriven1DPar(lonTargetResolution, MinMinLon);
		ArrayList<double[]> lonQueries = loadInputQuery(inputQueryPath, true);
		double[] alignCombineLonSolution = alignCombineLon.reshaping(totalNumLonBoundaries, lonSrcs, lonDatas,
				lonQueries);
		QueryDriven1DPar alignCombineLat = new QueryDriven1DPar(latTargetResolution, MinMinLat);
		ArrayList<double[]> latQueries = loadInputQuery(inputQueryPath, false);
		double[] alignCombineLatSolution = alignCombineLat.reshaping(totalNumLatBoundaries, latSrcs, latDatas,
				latQueries);
		ArrayList<double[]> alignCombineSolutions = new ArrayList<double[]>();
		alignCombineSolutions.add(alignCombineLonSolution);
		alignCombineSolutions.add(alignCombineLatSolution);
		System.out.println("AlignFunction lon bound size: " + alignCombineLonSolution.length + ", lat bound size: "
				+ alignCombineLatSolution.length);
		return alignCombineSolutions;
	}

	protected ArrayList<double[]> vOptSolution(SimpleSpatialHistogramOpt[] srcHists) {
		ArrayList<double[]> lonSrcs = new ArrayList<double[]>();
		ArrayList<double[]> latSrcs = new ArrayList<double[]>();
		ArrayList<double[]> lonFres = new ArrayList<double[]>();
		ArrayList<double[]> latFres = new ArrayList<double[]>();
		int totalNumLonBoundaries = 0;
		int totalNumLatBoundaries = 0;
		ArrayList<double[]> lonDatas = new ArrayList<double[]>();
		ArrayList<double[]> latDatas = new ArrayList<double[]>();
		for (int i = 0; i < srcHists.length; i++) {
			double[] lon = srcHists[i].getLonBoundary();
			lonSrcs.add(lon);
			totalNumLonBoundaries += lon.length;
			double[] lat = srcHists[i].getLatBoundary();
			latSrcs.add(lat);
			totalNumLatBoundaries += lat.length;

			lonFres.add(srcHists[i].getLonFrequency());
			latFres.add(srcHists[i].getLatFrequency());
			double[][] data = srcHists[i].getData();
			double[] lonData = new double[lon.length - 1];
			double[] latData = new double[lat.length - 1];
			for (int lonId = 0; lonId < data.length; lonId++) {
				for (int latId = 0; latId < data[0].length; latId++) {
					lonData[lonId] += data[lonId][latId];
					latData[latId] += data[lonId][latId];
				}
			}
			lonDatas.add(lonData);
			latDatas.add(latData);
		}

		ArrayList<double[]> vOptSolutions = new ArrayList<double[]>();
		NearOptimal1D vOptLonBS = new NearOptimal1D(lonTargetResolution, totalNumLonBoundaries, MinMinLon, MaxMaxLon);
		vOptSolutions.add(vOptLonBS.reshapping(lonSrcs, lonFres));
		NearOptimal1D vOptLatBS = new NearOptimal1D(latTargetResolution, totalNumLatBoundaries, MinMinLat, MaxMaxLat);
		vOptSolutions.add(vOptLatBS.reshapping(latSrcs, latFres));
//		System.out.println("VOptimal lon bound size: " + vOptSolutions.get(0).length + ", lat bound size: "
//				+ vOptSolutions.get(1).length);
		return vOptSolutions;
	}

	protected void rangeQueryEval(SimpleSpatialHistogramOpt[] srcHists, SimpleSpatialHistogramOpt[] tarHists,
			String histPath) throws IOException {
		SimpleSpatialHistogramOpt alignCombineHist = tarHists[0];
		SimpleSpatialHistogramOpt uniformHist = tarHists[1];
		SimpleSpatialHistogramOpt randomHist = tarHists[2];
		SimpleSpatialHistogramOpt vOptHist = tarHists[3];

		BetaDistribution betaUniform = computeBeta(uniformHist);
		BetaDistribution betaRand = computeBeta(randomHist);
		BetaDistribution betaAlign = computeBeta(alignCombineHist);
		BetaDistribution betaVOpt = computeBeta(vOptHist);

		BetaDistribution[] betaSrcHists = new BetaDistribution[srcHists.length];
		for (int sId = 0; sId < betaSrcHists.length; sId++) {
			betaSrcHists[sId] = computeBeta(srcHists[sId]);
		}
		String queryStr = "";
		String ratioStr = "";
		String overlapStr = "";

		String queryPath = histPath;
		System.out.println(queryPath);

		ArrayList<double[]> rangeQueries = loadRangeQuery(queryPath);

		double absAlignError = 0;
		double absUniformError = 0;
		double absRandError = 0;
		double absVOptError = 0;
		double absPartialSearchError = 0;

		double reshapesAlignError = 0;
		double reshapesUniformError = 0;
		double partialSearchError = 0;
		double reshapesRandError = 0;
		double reshapesVOptError = 0;

		double alignEstRatio = 0;
		double uniformEstRatio = 0;
		double randEstRatio = 0;
		double vOptEstRatio = 0;

		double alignRatio = 0;
		double uniformRatio = 0;
		double randRatio = 0;
		double vOptRatio = 0;

		int objSize = 0;
		for (int qId = 0; qId < rangeQueries.size(); qId++) {
			double[] query = rangeQueries.get(qId);
			double queryGT = 0;
			double[] randAns = null;
			double[] uniformAns = null;
			double[] alignAns = null;
			double[] vOptAns = null;
			double partialSearchAns = 0.0;

			queryGT = query[4];
			randAns = randomHist.RangeQueryWithRatio(betaUniform, query[0], query[1], query[2], query[3]);
			uniformAns = uniformHist.RangeQueryWithRatio(betaRand, query[0], query[1], query[2], query[3]);
			alignAns = alignCombineHist.RangeQueryWithRatio(betaAlign, query[0], query[1], query[2], query[3]);
			vOptAns = vOptHist.RangeQueryWithRatio(betaVOpt, query[0], query[1], query[2], query[3]);

			for (int sId = 0; sId < srcHists.length; sId++) {
				double[] singlePSAns = srcHists[sId].RangeQueryWithRatio(betaSrcHists[sId], query[0], query[1],
						query[2], query[3]);
				partialSearchAns += singlePSAns[0];
			}

			// compute error
			double uniformE = 0;
			double randE = 0;
			double alignE = 0;
			double vOptE = 0;
			double partialSearchE = 0;
			uniformE = Math.abs(queryGT - uniformAns[0]) / queryGT;
			randE = Math.abs(queryGT - randAns[0]) / queryGT;
			alignE = Math.abs(queryGT - alignAns[0]) / queryGT;
			vOptE = Math.abs(queryGT - vOptAns[0]) / queryGT;
			partialSearchE = Math.abs(queryGT - partialSearchAns) / queryGT;
//			if (alignE < 2.0 || uniformE < 2.0 || vOptE < 2.0) {
//			if (alignE < 2.0 || uniformE < 2.0) {
			reshapesAlignError += alignE;
			reshapesUniformError += uniformE;
			reshapesRandError += randE;
			reshapesVOptError += vOptE;
			partialSearchError += partialSearchE;

			absAlignError += Math.abs(queryGT - alignAns[0]);
			absUniformError += Math.abs(queryGT - uniformAns[0]);
			absRandError += Math.abs(queryGT - randAns[0]);
			absVOptError += Math.abs(queryGT - vOptAns[0]);
			absPartialSearchError += Math.abs(queryGT - partialSearchAns);

			alignEstRatio += alignAns[2];
			uniformEstRatio += uniformAns[2];
			randEstRatio += randAns[2];
			vOptEstRatio += vOptAns[2];

			alignRatio += alignAns[3];
			uniformRatio += uniformAns[3];
			randRatio += randAns[3];
			vOptRatio += vOptAns[3];

			objSize++;
//			}
		}

		int totalNumCells = (alignCombineHist.numLonBucket - 1) * (alignCombineHist.numLatBucket - 1);
		double overlapping = uniformRatio / objSize / totalNumCells;

		// compute rel error
		queryStr += String.format("%.1f", mergeRate * 100) + "," + String.format("%.3f", overlapping * 1000) + ","
				+ String.format("%.3f", (partialSearchError / objSize)) + ","
				+ String.format("%.3f", (reshapesAlignError / objSize)) + ","
				+ String.format("%.3f", reshapesUniformError / objSize) + ","
				+ String.format("%.3f", reshapesVOptError / objSize) + ","
				+ String.format("%.3f", (reshapesRandError / objSize)) + "\n";

		ratioStr += String.format("%.3f", 0.000) + "," + String.format("%.3f", alignEstRatio / objSize) + ","
				+ String.format("%.3f", uniformEstRatio / objSize) + "," + String.format("%.3f", randEstRatio / objSize)
				+ "," + String.format("%.3f", vOptEstRatio / objSize) + "\n";

		System.out.println("query size = " + objSize + ", total query size = " + rangeQueries.size());
		System.out.println("queryStr = ");
		System.out.println(queryStr);
		queryErrorWriter.write(queryStr);
	}

	protected void rangeQueryFilter(SimpleSpatialHistogramOpt[] srcHists, SimpleSpatialHistogramOpt[] tarHists,
			String histPath) throws IOException {
		SimpleSpatialHistogramOpt alignCombineHist = tarHists[0];
		SimpleSpatialHistogramOpt uniformHist = tarHists[1];
		SimpleSpatialHistogramOpt randomHist = tarHists[2];
		SimpleSpatialHistogramOpt vOptHist = tarHists[3];

		BetaDistribution betaUniform = computeBeta(uniformHist);
		BetaDistribution betaRand = computeBeta(randomHist);
		BetaDistribution betaAlign = computeBeta(alignCombineHist);
		BetaDistribution betaVOpt = computeBeta(vOptHist);

		BetaDistribution[] betaSrcHists = new BetaDistribution[srcHists.length];
		for (int sId = 0; sId < betaSrcHists.length; sId++) {
			betaSrcHists[sId] = computeBeta(srcHists[sId]);
		}
		String queryStr = "";
		String ratioStr = "";
		String overlapStr = "";

		String queryPath = histPath;
		System.out.println(queryPath);

		ArrayList<double[]> rangeQueries = loadRangeQuery(queryPath);
		String filterQueryPath = histPath;
		System.out.println(filterQueryPath);
		FileWriter writer = new FileWriter(filterQueryPath);

		double absAlignError = 0;
		double absUniformError = 0;
		double absRandError = 0;
		double absVOptError = 0;
		double absPartialSearchError = 0;

		double reshapesAlignError = 0;
		double reshapesUniformError = 0;
		double partialSearchError = 0;
		double reshapesRandError = 0;
		double reshapesVOptError = 0;

		double alignEstRatio = 0;
		double uniformEstRatio = 0;
		double randEstRatio = 0;
		double vOptEstRatio = 0;

		double alignRatio = 0;
		double uniformRatio = 0;
		double randRatio = 0;
		double vOptRatio = 0;

		int objSize = 0;
		for (int qId = 0; qId < rangeQueries.size(); qId++) {
			double[] query = rangeQueries.get(qId);
			double queryGT = 0;
			double[] randAns = null;
			double[] uniformAns = null;
			double[] alignAns = null;
			double[] vOptAns = null;
			double[] partialSearchAns = new double[4];

			queryGT = query[4];
			randAns = randomHist.RangeQueryWithRatio(betaUniform, query[0], query[1], query[2], query[3]);
			uniformAns = uniformHist.RangeQueryWithRatio(betaRand, query[0], query[1], query[2], query[3]);
			alignAns = alignCombineHist.RangeQueryWithRatio(betaAlign, query[0], query[1], query[2], query[3]);
			vOptAns = vOptHist.RangeQueryWithRatio(betaVOpt, query[0], query[1], query[2], query[3]);

			for (int sId = 0; sId < srcHists.length; sId++) {
				double[] singlePSAns = srcHists[sId].RangeQueryWithRatio(betaSrcHists[sId], query[0], query[1],
						query[2], query[3]);
				partialSearchAns[0] += singlePSAns[0];
			}

			// compute error
			double uniformE = 0;
			double randE = 0;
			double alignE = 0;
			double vOptE = 0;
			double partialSearchE = 0;
			uniformE = Math.abs(queryGT - uniformAns[0]) / queryGT;
			randE = Math.abs(queryGT - randAns[0]) / queryGT;
			alignE = Math.abs(queryGT - alignAns[0]) / queryGT;
			vOptE = Math.abs(queryGT - vOptAns[0]) / queryGT;
			partialSearchE = Math.abs(queryGT - partialSearchAns[0]) / queryGT;

			if (partialSearchE < 2.0 || alignE < 2.0 || uniformE < 2.0 || vOptE < 2.0) {
				String filteredQuery = query[0] + "," + query[1] + "," + query[2] + "," + query[3] + "," + query[4]
						+ "\n";
				writer.write(filteredQuery);
				reshapesAlignError += alignE;
				reshapesUniformError += uniformE;
				reshapesRandError += randE;
				reshapesVOptError += vOptE;
				partialSearchError += partialSearchE;

				absAlignError += Math.abs(queryGT - alignAns[0]);
				absUniformError += Math.abs(queryGT - uniformAns[0]);
				absRandError += Math.abs(queryGT - randAns[0]);
				absVOptError += Math.abs(queryGT - vOptAns[0]);
				absPartialSearchError += Math.abs(queryGT - partialSearchAns[0]);

				alignEstRatio += alignAns[2];
				uniformEstRatio += uniformAns[2];
				randEstRatio += randAns[2];
				vOptEstRatio += vOptAns[2];

				alignRatio += alignAns[3];
				uniformRatio += uniformAns[3];
				randRatio += randAns[3];
				vOptRatio += vOptAns[3];

				objSize++;
			}
		}
		writer.close();
		int totalNumCells = (alignCombineHist.numLonBucket - 1) * (alignCombineHist.numLatBucket - 1);
		double overlapping = uniformRatio / objSize / totalNumCells;

		// compute rel error
		queryStr += String.format("%.1f", mergeRate * 100) + "," + String.format("%.3f", overlapping * 1000) + ","
				+ String.format("%.3f", (partialSearchError / objSize)) + ","
				+ String.format("%.3f", (reshapesAlignError / objSize)) + ","
				+ String.format("%.3f", reshapesUniformError / objSize) + ","
				+ String.format("%.3f", reshapesVOptError / objSize) + ","
				+ String.format("%.3f", (reshapesRandError / objSize)) + "\n";

		ratioStr += String.format("%.3f", 0.000) + "," + String.format("%.3f", alignEstRatio / objSize) + ","
				+ String.format("%.3f", uniformEstRatio / objSize) + "," + String.format("%.3f", randEstRatio / objSize)
				+ "," + String.format("%.3f", vOptEstRatio / objSize) + "\n";

		System.out.println("query size = " + objSize + ", total query size = " + rangeQueries.size());
		System.out.println("queryStr = ");
		System.out.println(queryStr);
		queryErrorWriter.write(queryStr);
	}
	
	protected String rangeQueryEvalFullHist(SimpleSpatialHistogramOpt[] srcHists, SimpleSpatialHistogramOpt tarHist,
			String histPath, boolean doPartialSearch) throws IOException {
		BetaDistribution beta = computeBeta(tarHist);

		BetaDistribution[] betaSrcHists = new BetaDistribution[srcHists.length];
		for (int sId = 0; sId < betaSrcHists.length; sId++) {
			betaSrcHists[sId] = computeBeta(srcHists[sId]);
		}
		String queryStr = "";
		String ratioStr = "";
		String overlapStr = "";

		String queryPath = histPath;
//		System.out.println(queryPath);

		ArrayList<double[]> rangeQueries = loadRangeQuery(queryPath);
		
		if (!doPartialSearch) {
			double absError = 0;
			double reshapesError = 0;

			for (int qId = 0; qId < rangeQueries.size(); qId++) {
				double[] query = rangeQueries.get(qId);
				double queryGT = 0;
				double[] ans = null;

				queryGT = query[4];
				ans = tarHist.RangeQueryWithRatio(beta, query[0], query[1], query[2], query[3]);

				// compute error
				double alignE = 0;
				alignE = Math.abs(queryGT - ans[0]) / queryGT;
				reshapesError += alignE;
				absError += Math.abs(queryGT - ans[0]);

			}

			int totalNumCells = (tarHist.numLonBucket - 1) * (tarHist.numLatBucket - 1);

			// compute rel error
			queryStr += String.format("%.3f", (reshapesError / rangeQueries.size()));

//			System.out.println("query size = " + objSize + ", total query size = " + rangeQueries.size());
//			System.out.println("queryStr = ");
//			System.out.println(queryStr);
//			queryErrorWriter.write(queryStr);
			
			return queryStr;
		} else {
			double absPartialSearchError = 0;
			double partialSearchError = 0;

			for (int qId = 0; qId < rangeQueries.size(); qId++) {
				double[] query = rangeQueries.get(qId);
				double queryGT = 0;
				double partialSearchAns = 0.0;
				queryGT = query[4];	
				for (int sId = 0; sId < srcHists.length; sId++) {
					double[] singlePSAns = srcHists[sId].RangeQueryWithRatio(betaSrcHists[sId], query[0], query[1],
							query[2], query[3]);
					partialSearchAns += singlePSAns[0];
				}

				// compute error
				double partialSearchE = 0;
				partialSearchE = Math.abs(queryGT - partialSearchAns) / queryGT;
				partialSearchError += partialSearchE;
				absPartialSearchError += Math.abs(queryGT - partialSearchAns);
				
			}
			int totalNumCells = (tarHist.numLonBucket - 1) * (tarHist.numLatBucket - 1);
//			double overlapping = uniformRatio / objSize / totalNumCells;

			// compute rel error
			queryStr += String.format("%.1f", mergeRate * 100) + ","
					+ String.format("%.3f", (partialSearchError / rangeQueries.size()));

//			System.out.println("query size = " + objSize + ", total query size = " + rangeQueries.size());
//			System.out.println("queryStr = ");
//			System.out.println(queryStr);
//			queryErrorWriter.write(queryStr);
			return queryStr;
		}
	}

	protected BetaDistribution computeBeta(SimpleSpatialHistogramOpt mergeHist) {
		double[] mergeLon = mergeHist.getLonBoundary();
		double[] mergeLat = mergeHist.getLatBoundary();
		double[][] mergeData = mergeHist.getData();
		BetaDistribution mergeBetaDist = UtilsFunctionHistogram.ComputeBetaDist(mergeData, mergeLon, mergeLat,
				scaleFactor, varepsilon);
		return mergeBetaDist;
	}

	protected SimpleSpatialHistogramOpt[] loadSynopsisFromData() {
		SimpleSpatialHistogramOpt[] hists = new SimpleSpatialHistogramOpt[numOfFile];
		for (int fileId = 0; fileId < numOfFile; fileId++) {
			String inputPath = folder + fileId + ".csv";
			double minLon = Double.MAX_VALUE;
			double maxLon = -Double.MAX_VALUE;
			double minLat = Double.MAX_VALUE;
			double maxLat = -Double.MAX_VALUE;
			ArrayList<double[]> dataList = new ArrayList<double[]>();
			BufferedReader dataReader;
			try {
				dataReader = new BufferedReader(new FileReader(inputPath));
				String dataStr = dataReader.readLine();
				while (dataStr != null) {
					String[] tmp = dataStr.split("\t");
					double x = Double.parseDouble(tmp[0]);
					double y = Double.parseDouble(tmp[1]);
					double[] data = new double[2];
					data[0] = x;
					data[1] = y;
					dataList.add(data);
					MinMinLon = Math.min(MinMinLon, x);
					MaxMaxLon = Math.max(MaxMaxLon, x);
					MinMinLat = Math.min(MinMinLat, y);
					MaxMaxLat = Math.max(MaxMaxLat, y);
					minLon = Math.min(minLon, x);
					maxLon = Math.max(maxLon, x);
					minLat = Math.min(minLat, y);
					maxLat = Math.max(maxLat, y);
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
			minLon = minLon - varepsilon;
			maxLon = maxLon + varepsilon;
			minLat = minLat - varepsilon;
			maxLat = maxLat + varepsilon;
//			System.out.println("data size = " + dataList.size());
			SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(minLon, minLat, maxLon, maxLat,
					srcResolution[fileId], srcResolution[fileId]);
			for (int dId = 0; dId < dataList.size(); dId++) {
				double[] data = dataList.get(dId);
				hist.addRecord(data[0], data[1]);
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

	protected ArrayList<double[]> loadOriginalData() {
		ArrayList<double[]> dataList = new ArrayList<double[]>();
		String filePath = folder + "merge.csv";
		File file = new File(filePath);

		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(file));

			String line = reader.readLine();
			while (line != null) {
				String[] tmp = null;
				tmp = line.split("\t");

				double lon = Double.parseDouble(tmp[0]);

				double lat = Double.parseDouble(tmp[1]);

				double[] data = new double[2];
				data[0] = lon;
				data[1] = lat;
				dataList.add(data);

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
		return dataList;
	}

	protected ArrayList<double[]> loadRangeQuery(String queryPath) {
		ArrayList<double[]> queries = new ArrayList<double[]>();
		File file = new File(queryPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String str = reader.readLine();
			while (str != null) {
				String[] querytmp = str.split(",");
				// 0: qMinLon, 1: qMaxLon, 2: qMinLat, 3: qMaxLat,
				// 4: qGroundTruth
				double[] query = new double[5];
				for (int i = 0; i < 5; i++) {
					query[i] = Double.parseDouble(querytmp[i]);
				}
				queries.add(query);

				str = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return queries;

	}

	private ArrayList<double[]> loadInputQuery(String queryPath, boolean getLon) {
		ArrayList<double[]> queries = new ArrayList<double[]>();
		File file = new File(queryPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String str = reader.readLine();
			while (str != null) {
				String[] querytmp = str.split(",");

				double[] query = new double[3];
				if (getLon) {
					query[0] = Double.parseDouble(querytmp[0]);
					query[1] = Double.parseDouble(querytmp[1]);
				} else {
					query[0] = Double.parseDouble(querytmp[2]);
					query[1] = Double.parseDouble(querytmp[3]);
				}

				queries.add(query);

				str = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return queries;

	}

}
