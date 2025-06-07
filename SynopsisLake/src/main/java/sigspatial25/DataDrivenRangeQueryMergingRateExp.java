package sigspatial25;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import baseline.Random1D;
import histogram.SimpleSpatialHistogramOpt;
import utils.UtilsFunctionHistogram;

public class DataDrivenRangeQueryMergingRateExp extends CountHistExp {

	// 40-56-32-48
	int[] mergeResolutions = { 178, 179, 178, 42, 8 };

	public DataDrivenRangeQueryMergingRateExp(String folder, String srcHistPath, int numOfFile, int scoreFunction,
			double weightFactor, String rangeQueryPath) {
		super(folder, srcHistPath, numOfFile, scoreFunction, weightFactor, rangeQueryPath);
		int[] srcResolution = { 40, 56, 32, 48 };
		setSrcResolution(srcResolution);
		String queryErrorPath = folder + rangeQueryPath + "/results/DataDrivenQueryAvgRelError.txt";
		String histErrorPath = folder + rangeQueryPath + "/results/DataDrivenHistError.txt";
		try {
			queryErrorWriter = new FileWriter(queryErrorPath);
			queryErrorWriter.write("MergeRate(%),OverlappingRatio(10^-3),ReadOpt,ourLH,Uniform,Random,vMeasure\n");
			histErrorWriter = new FileWriter(histErrorPath);
			histErrorWriter.write("Resolution,MergeRate(%),ourLHRel,ourLHAbs,"
					+ "UniformRel,UniformAbs,RandomRel,RandomAbs,vMeasureRel,vMeasureAbs\n");
			for (int lonResol = mergeResolutions[2]; lonResol >= mergeResolutions[3]; lonResol -= mergeResolutions[4]) {
				this.lonTargetResolution = lonResol;
				this.latTargetResolution = lonResol;
				this.mergeRate = lonResol * lonResol / (double) mergeResolutions[0] / (double) mergeResolutions[1];
				if (lonResol == mergeResolutions[0]) {
					lonTargetResolution = mergeResolutions[0];
					latTargetResolution = mergeResolutions[1];
					this.mergeRate = 1.0;
				}

				SimpleSpatialHistogramOpt[] srcHists = loadSynopsisFromData();

				SimpleSpatialHistogramOpt[] tarHists = mergedPrecisionExp(mergeRate, srcHists);
				String histName = folder + rangeQueryPath + "srcQuery-40-56-32-48-Ans.txt";
				rangeQueryEval(srcHists, tarHists, histName);
//				double moveQueryRatio = 0.05;
//				String histName = folder + rangeQueryPath + "srcQuery-40-56-32-48-m" + moveQueryRatio + "Ans.txt";
//				rangeQueryFilter(srcHists, tarHists, histName);
			}
			queryErrorWriter.close();
			histErrorWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private SimpleSpatialHistogramOpt[] mergedPrecisionExp(double mergeRate, SimpleSpatialHistogramOpt[] srcHists)
			throws IOException {

		ArrayList<double[]> alignCombineSolutions = dataDrivenAlignSolution(srcHists);
		SimpleSpatialHistogramOpt alignCombineHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, alignCombineSolutions.get(0).length - 1, alignCombineSolutions.get(1).length - 1,
				alignCombineSolutions.get(0), alignCombineSolutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			alignCombineHist.aggregateHistogramSep(srcHists[sId]);
		}

		// uniform merge
		SimpleSpatialHistogramOpt uniformHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution);
		for (int sId = 0; sId < srcHists.length; sId++) {
			uniformHist.aggregateHistogramSep(srcHists[sId]);
		}

		// Random baseline
		ArrayList<double[]> randomBSSolution = new ArrayList<double[]>();
		Random1D randomLonBS = new Random1D(1.0, lonTargetResolution, MinMinLon, MaxMaxLon);
		randomBSSolution.add(randomLonBS.reshapping(10));
		Random1D randomLatBS = new Random1D(1.0, latTargetResolution, MinMinLat, MaxMaxLat);
		randomBSSolution.add(randomLatBS.reshapping(10));

		SimpleSpatialHistogramOpt randomHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
				randomBSSolution.get(0).length - 1, randomBSSolution.get(1).length - 1, randomBSSolution.get(0),
				randomBSSolution.get(1));
		for (int sId = 0; sId < srcHists.length; sId++) {
			randomHist.aggregateHistogramSep(srcHists[sId]);
		}

		// VOpt baseline
		ArrayList<double[]> vOptSolutions = vOptSolution(srcHists);
		SimpleSpatialHistogramOpt vOptHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
				vOptSolutions.get(0).length - 1, vOptSolutions.get(1).length - 1, vOptSolutions.get(0),
				vOptSolutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			vOptHist.aggregateHistogramSep(srcHists[sId]);
		}

		// original data
		ArrayList<double[]> dataList = loadOriginalData();
		System.out.println("total number of data points = " + dataList.size());

		// evaluation
		String histErrorStr = lonTargetResolution + "," + String.format("%.3f", (mergeRate)) + ",";
		System.out.println("---AlignCombine Evaluation---");
		SimpleSpatialHistogramOpt alignTargetHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, alignCombineSolutions.get(0),
				alignCombineSolutions.get(1));
		alignTargetHistsGT.loadHistByDataFile(alignTargetHistsGT, dataList);
//		System.out.println("total num of item in align GT = " + alignTargetHistsGT.totalN);
		UtilsFunctionHistogram.AlignmentErrorUpdate(scaleFactor, varepsilon, srcHists, alignCombineHist, scoreFunction,
				weightFactor, doNorm);
		histErrorStr += UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(alignTargetHistsGT,
				alignCombineHist);
		System.out.println();

		System.out.println("---Uniform Evaluation---");
		SimpleSpatialHistogramOpt uniformHistGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution);
		uniformHistGT.loadHistByDataFile(uniformHistGT, dataList);

//		System.out.println("total num of item in uniform GT = " + uniformHistGT.totalN);
		UtilsFunctionHistogram.AlignmentErrorUpdate(scaleFactor, varepsilon, srcHists, uniformHist, scoreFunction,
				weightFactor, doNorm);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(uniformHistGT, uniformHist);
		System.out.println();

		System.out.println("---Random Evaluation---");
		SimpleSpatialHistogramOpt randTargetHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, randomBSSolution.get(0), randomBSSolution.get(1));
		randTargetHistsGT.loadHistByDataFile(randTargetHistsGT, dataList);
		UtilsFunctionHistogram.AlignmentErrorUpdate(scaleFactor, varepsilon, srcHists, randomHist, scoreFunction,
				weightFactor, doNorm);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(randTargetHistsGT, randomHist);
		System.out.println();

		System.out.println("---VOptimal Evaluation---");
		SimpleSpatialHistogramOpt vOptHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, vOptSolutions.get(0), vOptSolutions.get(1));
		vOptHistsGT.loadHistByDataFile(vOptHistsGT, dataList);
		UtilsFunctionHistogram.AlignmentErrorUpdate(scaleFactor, varepsilon, srcHists, vOptHist, scoreFunction,
				weightFactor, doNorm);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(vOptHistsGT, vOptHist);
		System.out.println();

		SimpleSpatialHistogramOpt[] tarHists = new SimpleSpatialHistogramOpt[8];
		tarHists[0] = alignCombineHist;
		tarHists[1] = uniformHist;
		tarHists[2] = randomHist;
		tarHists[3] = vOptHist;
		tarHists[4] = alignTargetHistsGT;
		tarHists[5] = uniformHistGT;
		tarHists[6] = randTargetHistsGT;
		tarHists[7] = vOptHistsGT;

		System.out.println("histErrorStr = " + histErrorStr);
		histErrorWriter.write(histErrorStr + "\n");
		return tarHists;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/sigspatial/exp/range/";
		String srcHistPath = "";
		int numOfFile = 4;
		int scoreFunction = 2;
		double weightFactor = 0.5;
		String rangeQueryPath = "dataDrivenQuery/";
		DataDrivenRangeQueryMergingRateExp run = new DataDrivenRangeQueryMergingRateExp(
				folder, srcHistPath, numOfFile, scoreFunction, weightFactor, rangeQueryPath);
	}

}
