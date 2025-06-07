package sigspatial25;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import baseline.Random1D;
import histogram.SimpleSpatialHistogramOpt;
import utils.UtilsFunctionHistogram;

public class VaryingParameters extends CountHistExp {
	
	int[] mergeResolutions = { 178, 179, 178, 42, 8 };
	
	String inputQueryPath;
	
	public VaryingParameters(String folder, String srcHistPath, int numOfFile, String rangeQueryPath, String inputQueryName) {
		super(folder, srcHistPath, numOfFile, rangeQueryPath);
		this.inputQueryPath = folder + rangeQueryPath + inputQueryName;
		System.out.println("inputQueryPath = " + inputQueryPath);
		int[] srcResolution = { 40, 56, 32, 48 };
		setSrcResolution(srcResolution);
		double[] mergingRates = {1.0, 0.9, 0.8, 0.7, 0.6, 0.5};
		String queryErrorPath = folder + rangeQueryPath + "/results/ParameterQueryAvgRelError.txt";
		String histErrorPath = folder + rangeQueryPath + "/results/ParameterHistError.txt";
		try {
			queryErrorWriter = new FileWriter(queryErrorPath);
			queryErrorWriter.write("MergeRate(%),ReadOpt,queryDriven,alignOnly,densityOnly,"
					+ "combine25,combine50,combine75,Uniform,vMeasure\n");
			histErrorWriter = new FileWriter(histErrorPath);
			histErrorWriter.write("Resolution,MergeRate(%),queryDrivenRel,queryDriveAbs,"
					+ "alignOnlyRel,alignOnlyAbs,densityOnlyRel,densityOnlyAbs,"
					+ "combine25Rel,combine25Abs,combine50Rel,combine50Abs,combine75Rel,combine75Abs,"
					+ "uniformRel,uniformAbs,vMeasureRel,vMeasureAbs\n");
			for (int i=0; i<mergingRates.length; i++) {
				double mergeRate = mergingRates[i];
				this.lonTargetResolution = (int) (mergeResolutions[0] * mergeRate);
				this.latTargetResolution = (int) (mergeResolutions[01] * mergeRate);
				this.mergeRate = mergeRate;
				SimpleSpatialHistogramOpt[] srcHists = loadSynopsisFromData();
				SimpleSpatialHistogramOpt[] tarHists = mergedPrecisionExp(mergeRate, srcHists);
				String histName = folder + rangeQueryPath + "40-56-32-48.txt";
				String queryStr = "";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[0],histName, true) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[0],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[1],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[2],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[3],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[4],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[5],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[6],histName, false) + ",";
				queryStr += rangeQueryEvalFullHist(srcHists,tarHists[7],histName, false) + "\n";
				queryErrorWriter.write(queryStr);
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

		ArrayList<double[]> queryDrivenSolutions = queryDrivenAlignSolution(srcHists, inputQueryPath);
		SimpleSpatialHistogramOpt queryDrivenHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, queryDrivenSolutions.get(0).length - 1, queryDrivenSolutions.get(1).length - 1,
				queryDrivenSolutions.get(0), queryDrivenSolutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			queryDrivenHist.aggregateHistogramSep(srcHists[sId]);
		}
		
		setAlignParameters(1, -1);
		ArrayList<double[]> alignOnlySolutions = dataDrivenAlignSolution(srcHists);
		SimpleSpatialHistogramOpt alignOnlyHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, alignOnlySolutions.get(0).length - 1, alignOnlySolutions.get(1).length - 1,
				alignOnlySolutions.get(0), alignOnlySolutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			alignOnlyHist.aggregateHistogramSep(srcHists[sId]);
		}
		
		setAlignParameters(0, -1);
		ArrayList<double[]> densityOnlySolutions = dataDrivenAlignSolution(srcHists);
		SimpleSpatialHistogramOpt densityOnlyHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, densityOnlySolutions.get(0).length - 1, densityOnlySolutions.get(1).length - 1,
				densityOnlySolutions.get(0), densityOnlySolutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			densityOnlyHist.aggregateHistogramSep(srcHists[sId]);
		}
		setAlignParameters(2, 0.25);
		ArrayList<double[]> alignCombine25Solutions = dataDrivenAlignSolution(srcHists);
		SimpleSpatialHistogramOpt alignCombine25Hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, alignCombine25Solutions.get(0).length - 1, alignCombine25Solutions.get(1).length - 1,
				alignCombine25Solutions.get(0), alignCombine25Solutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			alignCombine25Hist.aggregateHistogramSep(srcHists[sId]);
		}
		setAlignParameters(2, 0.5);
		ArrayList<double[]> alignCombine50Solutions = dataDrivenAlignSolution(srcHists);
		SimpleSpatialHistogramOpt alignCombine50Hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, alignCombine50Solutions.get(0).length - 1, alignCombine50Solutions.get(1).length - 1,
				alignCombine50Solutions.get(0), alignCombine50Solutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			alignCombine50Hist.aggregateHistogramSep(srcHists[sId]);
		}
		setAlignParameters(2, 0.75);
		ArrayList<double[]> alignCombine75Solutions = dataDrivenAlignSolution(srcHists);
		SimpleSpatialHistogramOpt alignCombine75Hist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, alignCombine75Solutions.get(0).length - 1, alignCombine75Solutions.get(1).length - 1,
				alignCombine75Solutions.get(0), alignCombine75Solutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			alignCombine75Hist.aggregateHistogramSep(srcHists[sId]);
		}

		// uniform merge
		SimpleSpatialHistogramOpt uniformHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution);
		for (int sId = 0; sId < srcHists.length; sId++) {
			uniformHist.aggregateHistogramSep(srcHists[sId]);
		}

		// VOpt baseline
		ArrayList<double[]> vOptSolutions = vOptSolution(srcHists);
		SimpleSpatialHistogramOpt vOptHist = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon, MaxMaxLat,
				vOptSolutions.get(0).length - 1, vOptSolutions.get(1).length - 1, vOptSolutions.get(0),
				vOptSolutions.get(1));

		for (int sId = 0; sId < srcHists.length; sId++) {
			vOptHist.aggregateHistogramSep(srcHists[sId]);
		}
		
		SimpleSpatialHistogramOpt queryDrivenHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, queryDrivenSolutions.get(0),
				queryDrivenSolutions.get(1));

		SimpleSpatialHistogramOpt alignOnlyHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, alignOnlySolutions.get(0),
				alignOnlySolutions.get(1));
		SimpleSpatialHistogramOpt densityOnlyHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, densityOnlySolutions.get(0),
				densityOnlySolutions.get(1));
		
		SimpleSpatialHistogramOpt align25HistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, alignCombine25Solutions.get(0),
				alignCombine25Solutions.get(1));
		SimpleSpatialHistogramOpt align50HistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, alignCombine50Solutions.get(0),
				alignCombine50Solutions.get(1));
		SimpleSpatialHistogramOpt align75HistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, alignCombine75Solutions.get(0),
				alignCombine75Solutions.get(1));
		
		SimpleSpatialHistogramOpt uniformHistGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution);
		SimpleSpatialHistogramOpt vOptHistsGT = new SimpleSpatialHistogramOpt(MinMinLon, MinMinLat, MaxMaxLon,
				MaxMaxLat, lonTargetResolution, latTargetResolution, vOptSolutions.get(0), vOptSolutions.get(1));
		
		// original data
		ArrayList<double[]> dataList = loadOriginalData();
		System.out.println("total number of data points = " + dataList.size());
//		ArrayList<double[]> evalQueries = loadRangeQuery(inputQueryPath);
//			this.histObjScores = new double[3];
		// evaluation
		String histErrorStr = lonTargetResolution + "," + String.format("%.3f", (mergeRate)) + ",";
		
		System.out.println("---QueryDriven Evaluation---");
		queryDrivenHistsGT.loadHistByDataFile(queryDrivenHistsGT, dataList);
		histErrorStr += UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(queryDrivenHistsGT,
				queryDrivenHist);
		System.out.println();
		
		System.out.println("---AlignOnly Evaluation---");
		alignOnlyHistsGT.loadHistByDataFile(alignOnlyHistsGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(alignOnlyHistsGT,
				alignOnlyHist);
		System.out.println();
		
		System.out.println("---DensityOnly Evaluation---");
		densityOnlyHistsGT.loadHistByDataFile(densityOnlyHistsGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(densityOnlyHistsGT,
				densityOnlyHist);
		System.out.println();
		
		System.out.println("---Align25 Evaluation---");
		align25HistsGT.loadHistByDataFile(align25HistsGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(align25HistsGT,
				alignCombine25Hist);
		System.out.println();
		
		System.out.println("---Align50 Evaluation---");
		align50HistsGT.loadHistByDataFile(align50HistsGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(align50HistsGT,
				alignCombine50Hist);
		System.out.println();
		
		System.out.println("---Align75 Evaluation---");
		align75HistsGT.loadHistByDataFile(align75HistsGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(align75HistsGT,
				alignCombine75Hist);
		System.out.println();

		System.out.println("---Uniform Evaluation---");

		uniformHistGT.loadHistByDataFile(uniformHistGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(uniformHistGT, uniformHist);
		System.out.println();
		
		System.out.println("---VOptimal Evaluation---");
		vOptHistsGT.loadHistByDataFile(vOptHistsGT, dataList);
		histErrorStr += "," + UtilsFunctionHistogram.SimpleSpatialHistogramOptEvaluation(vOptHistsGT, vOptHist) + "\n";
		System.out.println();

		System.out.println("histErrorStr = " + histErrorStr);
		histErrorWriter.write(histErrorStr);

		SimpleSpatialHistogramOpt[] tarHists = new SimpleSpatialHistogramOpt[8];
		tarHists[0] = queryDrivenHist;
		tarHists[1] = alignOnlyHist;
		tarHists[2] = densityOnlyHist;
		tarHists[3] = alignCombine25Hist;
		tarHists[4] = alignCombine50Hist;
		tarHists[5] = alignCombine75Hist;
		tarHists[6] = uniformHist;
		tarHists[7] = vOptHistsGT;
		return tarHists;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/sigspatial/exp/range/";
		String srcHistPath = "";
		int numOfFile = 4;
		String rangeQueryPath = "parameter/";
		String inputQueryName = "inputQuery40-56-32-48.txt";//"inputQuery20-28-16-24.txt";
		VaryingParameters run = new VaryingParameters(folder, srcHistPath,
				numOfFile, rangeQueryPath, inputQueryName);
	}
}
