package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.math3.distribution.BetaDistribution;

import dataStructure.SSH2DCell;
import dataStructure.objFunction.Interval;
import histogram.GeometricHistogramOpt;
import histogram.SimpleHistogramOpt;
import histogram.SimpleSpatialHistogram;
import histogram.SimpleSpatialHistogramOpt;

public class UtilsFunctionHistogram {

	/**
	 * Implemented based on https://www.geeksforgeeks.org/binary-search/
	 * 
	 * @param key:  search key
	 * @param data: data array
	 * @return index of the search key
	 */
	public static int BinarySearch(double key, double[] data) {
		int l = 0, r = data.length - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;

			// Check if x is present at mid
			if (data[m] == key) {
				return m;
			}

			// If x greater, ignore left half
			if (data[m] < key) {
				l = m + 1;
			}

			// If x is smaller, ignore right half
			else {
				r = m - 1;
			}
		}

		// if we reach here, then element was
		// not present
		return -1;

	}

	public static int FirstBiggerSearch(double key, double[] data) {
		int l = 0, r = data.length - 1;
		while (l <= r) {
			int m = l + (r - l) / 2;
//			System.out.println(m);

			// Check if x is present at mid
			if (data[m] == key) {
				return m;
			}

			// If x greater, ignore left half
			if (data[m] < key) {
				l = m + 1;
			}

			// If x is smaller, ignore right half
			else {
				r = m - 1;
			}
		}

//		if (l>= r) {
//			System.out.println(r + ", " + l);
//			System.out.println(data[r] + ", " + data[l] + ", " + key);
//		}

		// if we reach here, then element was
		// not present
		return Math.max(r, 0);

	}

	public static String SimpleHistogramOptEvaluation(SimpleHistogramOpt gt, SimpleHistogramOpt hist) {

		double relError = 0.0;
		double relErrorNoZero = 0.0;
		int relNumZero = 0;
		double modifiedRelError = 0.0;
		double modifiedRelErrorNoZero = 0.0;
		int modiRelNumZero = 0;
		double absError = 0.0;

		double[] data = hist.getData();
		double[] gtData = gt.getData();

		double totalGT = 0;
		double totalData = 0;
		for (int i = 0; i < data.length; i++) {
			totalGT += gtData[i];
			totalData += data[i];
			absError += Math.abs(gtData[i] - data[i]);
			if (gtData[i] == 0 && Math.abs(data[i]) > 1.1102230246251565E-10) {
				relError += Math.abs(data[i]);
				relNumZero++;
			} else if (gtData[i] != 0) {
				if (Math.abs((gtData[i] - data[i])) > 1.1102230246251565E-10) {
					double e = Math.abs((gtData[i] - data[i]) / gtData[i]);
					relError += e;
					relErrorNoZero += e;
				}
			}
			double me = Math.abs(gtData[i] - data[i]);
			double tmp = Math.min(gtData[i], data[i]);
			if (tmp == 0) {
				modiRelNumZero++;
				modifiedRelError += me;
			} else {
				if (tmp > 1.1102230246251565E-10) {
					me = me / tmp;
					modifiedRelError += me;
					modifiedRelErrorNoZero += me;
				}

			}

		}

		int cellNum = gt.numBucket;

		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
		System.out.println("cellNum = " + cellNum);

		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total relative error = " + relError + ", avg relative error = " + (relError / cellNum);

		System.out.println(evaluation);
		evaluation = "total abs error = " + absError + ", avg abs error = " + (absError / cellNum);

		System.out.println(evaluation);

		System.out.println("--------");
		return relError + "," + (relError / cellNum) + "," + relErrorNoZero + ","
				+ (relErrorNoZero / (cellNum - relNumZero)) + "," + relNumZero + "," + modifiedRelError + ","
				+ (modifiedRelError / cellNum) + "," + modifiedRelErrorNoZero + ","
				+ (modifiedRelErrorNoZero / (cellNum - modiRelNumZero)) + "," + modifiedRelErrorNoZero + "," + absError
				+ "," + (absError / cellNum);

	}

	public static String TwoSimpleSpatialHistogramOptEvaluation(SimpleSpatialHistogramOpt gt,
			SimpleSpatialHistogramOpt hist, SimpleSpatialHistogramOpt uniformGT, SimpleSpatialHistogramOpt uniform) {

		double relError = 0.0;
		double absError = 0.0;
		double[][] data = hist.getData();
		double[][] gtData = gt.getData();
		double totalGT = 0;
		double totalData = 0;

		double relErrorUniform = 0.0;
		double absErrorUniform = 0.0;
		double[][] dataUniform = uniform.getData();
		double[][] gtDataUniform = uniformGT.getData();
		double totalGTUniform = 0;
		double totalDataUniform = 0;

		double badRelError = 0.0;
		double badAbsError = 0.0;
		double badRelErrorUniform = 0.0;
		double badAbsErrorUniform = 0.0;

		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				totalGT += gtData[i][j];
				totalData += data[i][j];
				double curAbsError = Math.abs(gtData[i][j] - data[i][j]);
				absError += curAbsError;

				totalGTUniform += gtDataUniform[i][j];
				totalDataUniform += dataUniform[i][j];
				double curAbsErrorUniform = Math.abs(gtDataUniform[i][j] - dataUniform[i][j]);
				absErrorUniform += curAbsErrorUniform;

				double curRelError = 0.0;
				if (gtData[i][j] == 0 && Math.abs(data[i][j]) > 1.1102230246251565E-10) {
					curRelError = Math.abs(data[i][j]);
					relError += curRelError;
				} else if (gtData[i][j] != 0) {
					if (Math.abs((gtData[i][j] - data[i][j])) > 1.1102230246251565E-10) {
						curRelError = Math.abs((gtData[i][j] - data[i][j]) / gtData[i][j]);
						relError += curRelError;
					}
				}

				double curRelErrorUniform = 0.0;
				if (gtDataUniform[i][j] == 0 && Math.abs(dataUniform[i][j]) > 1.1102230246251565E-10) {
					curRelErrorUniform = Math.abs(dataUniform[i][j]);
					relErrorUniform += curRelErrorUniform;
				} else if (gtDataUniform[i][j] != 0) {
					if (Math.abs((gtDataUniform[i][j] - dataUniform[i][j])) > 1.1102230246251565E-10) {
						curRelErrorUniform = Math.abs((gtDataUniform[i][j] - dataUniform[i][j]) / gtDataUniform[i][j]);
						relErrorUniform += curRelErrorUniform;
					}
				}

				if (curAbsError > curAbsErrorUniform) {
					badAbsError += curAbsError;
					badAbsErrorUniform += curAbsErrorUniform;
				}

				if (curRelError > curRelErrorUniform) {
					badRelError += curRelError;
					badRelErrorUniform += curRelErrorUniform;
				}
			}
		}

		int cellNum = gt.numLonBucket * gt.numLatBucket;

		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
		System.out.println("cellNum = " + cellNum);

		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total relative error = " + String.format("%.3f", relError) + ", avg relative error = "
				+ String.format("%.3f", relError / cellNum);
		System.out.println(evaluation);
		evaluation = "total abs error = " + String.format("%.3f", absError) + ", avg abs error = "
				+ String.format("%.3f", absError / cellNum);

		System.out.println(evaluation);

		System.out.println("--------");
		double goodRelError = relError - badRelError;
		double goodRelErrorUniform = relErrorUniform - badRelErrorUniform;
		
		double goodAbsError = absError - badAbsError;
		double goodAbsErrorUniform = absErrorUniform - badAbsErrorUniform;
		
//		String resultStr = String.format("%.3f", relError) + "," + String.format("%.3f", absError);
		String resultStr = "totalRel," + String.format("%.3f", relError) + "," + String.format("%.3f", relErrorUniform)
				+ "\n" + "goodRel," + String.format("%.3f", goodRelError) + ","
				+ String.format("%.3f", goodRelErrorUniform) + "\n" + "badRel," + String.format("%.3f", badRelError)
				+ "," + String.format("%.3f", badRelErrorUniform) + "\n" + "totalAbs," + String.format("%.3f", absError)
				+ "," + String.format("%.3f", absErrorUniform) + "\n" + "goodAbs," + String.format("%.3f", goodAbsError)
				+ "," + String.format("%.3f", goodAbsErrorUniform) + "\n" + "badAbs,"
				+ String.format("%.3f", badAbsError) + "," + String.format("%.3f", badAbsErrorUniform) + "\n";
		return resultStr;
	}

	public static String SimpleSpatialHistogramOptEvaluation(SimpleSpatialHistogramOpt gt,
			SimpleSpatialHistogramOpt hist) {

		double relError = 0.0;
		double relErrorNoZero = 0.0;
		int relNumZero = 0;
		double modifiedRelError = 0.0;
		double modifiedRelErrorNoZero = 0.0;
		int modiRelNumZero = 0;
		double absError = 0.0;

		double[][] data = hist.getData();
		double[][] gtData = gt.getData();

		double totalGT = 0;
		double totalData = 0;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				totalGT += gtData[i][j];
				totalData += data[i][j];
				absError += Math.abs(gtData[i][j] - data[i][j]);
//				System.out.println(gtData[i][j] + ", " + data[i][j]);
				if (gtData[i][j] == 0 && Math.abs(data[i][j]) > 1.1102230246251565E-10) {
					relError += Math.abs(data[i][j]);
					relNumZero++;
//					System.out.println("rel = " +  Math.abs(data[i][j]));
				} else if (gtData[i][j] != 0) {
					if (Math.abs((gtData[i][j] - data[i][j])) > 1.1102230246251565E-10) {
						double e = Math.abs((gtData[i][j] - data[i][j]) / gtData[i][j]);
						relError += e;
						relErrorNoZero += e;
//						System.out.println("rel = " + e);
					}
				}
				double me = Math.abs(gtData[i][j] - data[i][j]);
				double tmp = Math.min(gtData[i][j], data[i][j]);
				if (tmp == 0) {
					modiRelNumZero++;
					modifiedRelError += me;
				} else {
					if (tmp > 1.1102230246251565E-10) {
						me = me / tmp;
						modifiedRelError += me;
						modifiedRelErrorNoZero += me;
					}

				}
			}
		}

		int cellNum = gt.numLonBucket * gt.numLatBucket;

		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
		System.out.println("cellNum = " + cellNum);

		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total relative error = " + String.format("%.3f", relError) + ", avg relative error = "
				+ String.format("%.3f", relError / cellNum);
		System.out.println(evaluation);
		evaluation = "total abs error = " + String.format("%.3f", absError) + ", avg abs error = "
				+ String.format("%.3f", absError / cellNum);

		System.out.println(evaluation);

		System.out.println("--------");
		String resultStr = String.format("%.3f", relError / cellNum) + "," + String.format("%.3f", absError / cellNum);
		return resultStr;
	}

	public static String SimpleSpatialHistogramOptEvaluation(SimpleSpatialHistogramOpt[] gts,
			SimpleSpatialHistogramOpt[] hists) {

		System.out.println("----SimpleSpatialHistogramOpt Total Evaluation----");

		double relErrorTotal = 0.0;
		double relErrorAvgTotal = 0.0;
		double relErrorNoZeroTotal = 0.0;
		double relErrorNoZeroAvgTotal = 0.0;
		double modifiedRelErrorTotal = 0.0;
		double modifiedRelErrorAvgTotal = 0.0;
		double modifiedRelErrorNoZeroTotal = 0.0;
		double modifiedRelErrorNoZeroAvgTotal = 0.0;
		double absErrorTotal = 0.0;
		double absErrorAvgTotal = 0.0;

		for (int hId = 0; hId < gts.length; hId++) {
			double relError = 0.0;
			double relErrorNoZero = 0.0;
			int relNumZero = 0;
			double modifiedRelError = 0.0;
			double modifiedRelErrorNoZero = 0.0;
			int modiRelNumZero = 0;
			double absError = 0.0;

			double[][] data = hists[hId].getData();
			double[][] gtData = gts[hId].getData();

			double totalGT = 0;
			double totalData = 0;
			for (int i = 0; i < data.length; i++) {
				for (int j = 0; j < data[0].length; j++) {
					totalGT += gtData[i][j];
					totalData += data[i][j];
					absError += Math.abs(gtData[i][j] - data[i][j]);
					if (gtData[i][j] == 0 && Math.abs(data[i][j]) > 1.1102230246251565E-10) {
						relError += Math.abs(data[i][j]);
						relNumZero++;
					} else if (gtData[i][j] != 0) {
						if (Math.abs((gtData[i][j] - data[i][j])) > 1.1102230246251565E-10) {
							double e = Math.abs((gtData[i][j] - data[i][j]) / gtData[i][j]);
							relError += e;
							relErrorNoZero += e;
						}
					}
					double me = Math.abs(gtData[i][j] - data[i][j]);
					double tmp = Math.min(gtData[i][j], data[i][j]);
					if (tmp == 0) {
						modiRelNumZero++;
						modifiedRelError += me;
					} else {
						if (tmp > 1.1102230246251565E-10) {
							me = me / tmp;
							modifiedRelError += me;
							modifiedRelErrorNoZero += me;
						}

					}
				}
			}

			int cellNum = gts[hId].numLonBucket * gts[hId].numLatBucket;

//			System.out.println("----SimpleSpatialHistogramOpt Single Evaluation----");
//			
//			System.out.println("cellNum = " + cellNum);
//
//			System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
//			String evaluation = "total relative error = " + relError + ", avg relative error = " + (relError / cellNum);
//
//			System.out.println(evaluation);
//			evaluation = "total abs error = " + absError + ", avg abs error = " + (absError / cellNum);
//
//			
//			System.out.println(evaluation);
//
//			System.out.println("--------");

			relErrorTotal += relError;
			relErrorAvgTotal += (relError / cellNum);
			relErrorNoZeroTotal += relErrorNoZero;
			relErrorNoZeroAvgTotal += relErrorNoZero / cellNum;
			modifiedRelErrorTotal += modifiedRelError;
			modifiedRelErrorAvgTotal += modifiedRelError / cellNum;
			modifiedRelErrorNoZeroTotal += modifiedRelErrorNoZero;
			modifiedRelErrorNoZeroAvgTotal += modifiedRelErrorNoZero / cellNum;
			absErrorTotal += absError;
			absErrorAvgTotal += (absError / cellNum);
		}

		String evaluation = "total relative error = " + relErrorTotal + ", avg relative error = "
				+ (relErrorAvgTotal / hists.length);
		System.out.println(evaluation);
		evaluation = "total abs error = " + absErrorTotal + ", avg abs error = " + (absErrorAvgTotal / hists.length);
		System.out.println(evaluation);

		return relErrorTotal + "," + (relErrorAvgTotal / hists.length) + "," + relErrorNoZeroTotal + ","
				+ (relErrorNoZeroAvgTotal / hists.length) + "," + modifiedRelErrorTotal + ","
				+ (modifiedRelErrorAvgTotal / hists.length) + "," + modifiedRelErrorNoZeroTotal + ","
				+ (modifiedRelErrorNoZeroAvgTotal / hists.length) + "," + absErrorTotal + ","
				+ (absErrorAvgTotal / hists.length);

	}

	public static String GeometricHistogramOptEvaluation(GeometricHistogramOpt[] gts, GeometricHistogramOpt[] hists) {

		System.out.println("----GeometricHistogramOpt Total Evaluation----");

		double relErrorTotal = 0.0;
		double relErrorAvgTotal = 0.0;
		double relErrorNoZeroTotal = 0.0;
		double relErrorNoZeroAvgTotal = 0.0;
		double modifiedRelErrorTotal = 0.0;
		double modifiedRelErrorAvgTotal = 0.0;
		double modifiedRelErrorNoZeroTotal = 0.0;
		double modifiedRelErrorNoZeroAvgTotal = 0.0;
		double absErrorTotal = 0.0;
		double absErrorAvgTotal = 0.0;

		for (int hId = 0; hId < gts.length; hId++) {
			double relError = 0.0;
			double relErrorNoZero = 0.0;
			int relNumZero = 0;
			double modifiedRelError = 0.0;
			double modifiedRelErrorNoZero = 0.0;
			int modiRelNumZero = 0;
			double absError = 0.0;

			double[][] data = hists[hId].getArea();
			double[][] gtData = gts[hId].getArea();

//			double[][] area = hists[hId].getArea();
//			double[][] gtArea = gts[hId].getArea();
//
//			double[][] SumRatioH = hists[hId].getRatioH();
//			double[][] gtSumRatioH = gts[hId].getRatioH();
//
//			double[][] SumRatioV = hists[hId].getRatioV();
//			double[][] gtSumRatioV = gts[hId].getRatioV();

			double totalGT = 0;
			double totalData = 0;
			for (int i = 0; i < data.length; i++) {
				for (int j = 0; j < data[0].length; j++) {
					totalGT += gtData[i][j];
					totalData += data[i][j];
					absError += Math.abs(gtData[i][j] - data[i][j]);
					if (gtData[i][j] == 0 && Math.abs(data[i][j]) > 1.1102230246251565E-10) {
						relError += Math.abs(data[i][j]);
						relNumZero++;
					} else if (gtData[i][j] != 0) {
						if (Math.abs((gtData[i][j] - data[i][j])) > 1.1102230246251565E-10) {
							double e = Math.abs((gtData[i][j] - data[i][j]) / gtData[i][j]);
							relError += e;
							relErrorNoZero += e;
						}
					}
					double me = Math.abs(gtData[i][j] - data[i][j]);
					double tmp = Math.min(gtData[i][j], data[i][j]);
					if (tmp == 0) {
						modiRelNumZero++;
						modifiedRelError += me;
					} else {
						if (tmp > 1.1102230246251565E-10) {
							me = me / tmp;
							modifiedRelError += me;
							modifiedRelErrorNoZero += me;
						}

					}
				}
			}

			int cellNum = gts[hId].numLonBucket * gts[hId].numLatBucket;
//			System.out.println(cellNum);
			relErrorTotal += relError;
			relErrorAvgTotal += (relError / cellNum);
			relErrorNoZeroTotal += relErrorNoZero;
			relErrorNoZeroAvgTotal += relErrorNoZero / cellNum;
			modifiedRelErrorTotal += modifiedRelError;
			modifiedRelErrorAvgTotal += modifiedRelError / cellNum;
			modifiedRelErrorNoZeroTotal += modifiedRelErrorNoZero;
			modifiedRelErrorNoZeroAvgTotal += modifiedRelErrorNoZero / cellNum;
			absErrorTotal += absError;
			absErrorAvgTotal += (absError / cellNum);
		}

		String evaluation = "total relative error = " + relErrorTotal + ", avg relative error = "
				+ (relErrorAvgTotal / hists.length);
//		System.out.println(evaluation);
		evaluation = "total abs error = " + absErrorTotal + ", avg abs error = " + (absErrorAvgTotal / hists.length);
//		System.out.println(evaluation);

		return relErrorTotal + "," + (relErrorAvgTotal / hists.length) + "," + relErrorNoZeroTotal + ","
				+ (relErrorNoZeroAvgTotal / hists.length) + "," + modifiedRelErrorTotal + ","
				+ (modifiedRelErrorAvgTotal / hists.length) + "," + modifiedRelErrorNoZeroTotal + ","
				+ (modifiedRelErrorNoZeroAvgTotal / hists.length) + "," + absErrorTotal + ","
				+ (absErrorAvgTotal / hists.length);

	}

	public static String GHOptEvaluation(GeometricHistogramOpt hist, GeometricHistogramOpt gt) {

		double relErrorCount = 0.0;
		double absErrorCount = 0.0;

		double relErrorArea = 0.0;
		double absErrorArea = 0.0;

		double relErrorRH = 0.0;
		double absErrorRH = 0.0;

		double relErrorRV = 0.0;
		double absErrorRV = 0.0;

		double[][] count = hist.getCount();
		double[][] gtCount = gt.getCount();

		double[][] area = hist.getArea();
		double[][] gtArea = gt.getArea();

		double[][] SumRatioH = hist.getRatioH();
		double[][] gtSumRatioH = gt.getRatioH();

		double[][] SumRatioV = hist.getRatioV();
		double[][] gtSumRatioV = gt.getRatioV();

		double totalGT = 0;
		double totalData = 0;
		
//		System.out.println("hist total N = " + hist.totalN);
//		System.out.println("GT total N = " + gt.totalN);
//		System.out.println();
		for (int i = 0; i < count.length; i++) {
			for (int j = 0; j < count[0].length; j++) {
				// count
				totalGT += gtCount[i][j];
				totalData += count[i][j];
				absErrorCount += Math.abs(gtCount[i][j] - count[i][j]);
				if (gtCount[i][j] == 0 && Math.abs(count[i][j]) > 1.1102230246251565E-10) {
					relErrorCount += Math.abs(count[i][j]);

				} else if (gtCount[i][j] != 0) {
					if (Math.abs((gtCount[i][j] - count[i][j])) > 1.1102230246251565E-10) {
						double e = Math.abs((gtCount[i][j] - count[i][j]) / gtCount[i][j]);
						relErrorCount += e;

					}
				}

				// area
				absErrorArea += Math.abs(gtArea[i][j] - area[i][j]);
				if (gtArea[i][j] == 0 && Math.abs(area[i][j]) > 1.1102230246251565E-10) {
					relErrorArea += Math.abs(area[i][j]);

				} else if (gtArea[i][j] != 0) {
					if (Math.abs((gtArea[i][j] - area[i][j])) > 1.1102230246251565E-10) {
						double e = Math.abs((gtArea[i][j] - area[i][j]) / gtArea[i][j]);
						relErrorArea += e;

					}
				}

				// RH
				absErrorRH += Math.abs(gtSumRatioH[i][j] - SumRatioH[i][j]);
				if (gtSumRatioH[i][j] == 0 && Math.abs(SumRatioH[i][j]) > 1.1102230246251565E-10) {
					relErrorRH += Math.abs(SumRatioH[i][j]);

				} else if (gtSumRatioH[i][j] != 0) {
					if (Math.abs((gtSumRatioH[i][j] - SumRatioH[i][j])) > 1.1102230246251565E-10) {
						double e = Math.abs((gtSumRatioH[i][j] - SumRatioH[i][j]) / gtSumRatioH[i][j]);
						relErrorRH += e;

					}
				}

				// RV
				absErrorRV += Math.abs(gtSumRatioV[i][j] - SumRatioV[i][j]);
				if (gtSumRatioV[i][j] == 0 && Math.abs(SumRatioV[i][j]) > 1.1102230246251565E-10) {
					relErrorRV += Math.abs(SumRatioV[i][j]);

				} else if (gtSumRatioV[i][j] != 0) {
					if (Math.abs((gtSumRatioV[i][j] - SumRatioV[i][j])) > 1.1102230246251565E-10) {
						double e = Math.abs((gtSumRatioV[i][j] - SumRatioV[i][j]) / gtSumRatioV[i][j]);
						relErrorRV += e;

					}
				}
			}
		}

		int cellNum = gt.numLonBucket * gt.numLatBucket;

//		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
//		System.out.println("cellNum = " + cellNum);
//
//		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total count relative error = " + relErrorCount + ", avg relative error = "
				+ (relErrorCount / cellNum);
//		System.out.println(evaluation);
		evaluation = "total abs count error = " + absErrorCount + ", avg abs error = " + (absErrorCount / cellNum);
//		System.out.println(evaluation);

		evaluation = "total area relative error = " + relErrorArea + ", avg relative error = "
				+ (relErrorArea / cellNum);
//		System.out.println(evaluation);
		evaluation = "total area count error = " + absErrorArea + ", avg abs error = " + (absErrorArea / cellNum);
//		System.out.println(evaluation);

		evaluation = "total RH relative error = " + relErrorRH + ", avg relative error = " + (relErrorRH / cellNum);
//		System.out.println(evaluation);
		evaluation = "total RH count error = " + absErrorRH + ", avg abs error = " + (absErrorRH / cellNum);
//		System.out.println(evaluation);

		evaluation = "total RV relative error = " + relErrorRV + ", avg relative error = " + (relErrorRV / cellNum);
//		System.out.println(evaluation);
		evaluation = "total RV count error = " + absErrorRV + ", avg abs error = " + (absErrorRV / cellNum);
//		System.out.println(evaluation);

//		System.out.println("--------");
		return evaluation;

	}

	public static double SimpleSpatialHistogramOptRelError(SimpleSpatialHistogramOpt gt,
			SimpleSpatialHistogramOpt hist) {

		double relError = 0.0;
		double relErrorNoZero = 0.0;
		int relNumZero = 0;
		double modifiedRelError = 0.0;
		double modifiedRelErrorNoZero = 0.0;
		int modiRelNumZero = 0;
		double absError = 0.0;

		double[][] data = hist.getData();
		double[][] gtData = gt.getData();

		double totalGT = 0;
		double totalData = 0;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
				totalGT += gtData[i][j];
				totalData += data[i][j];
				absError += Math.abs(gtData[i][j] - data[i][j]);
				if (gtData[i][j] == 0 && Math.abs(data[i][j]) > 1.1102230246251565E-10) {
					relError += Math.abs(data[i][j]);
					relNumZero++;
				} else if (gtData[i][j] != 0) {
					if (Math.abs((gtData[i][j] - data[i][j])) > 1.1102230246251565E-10) {
						double e = Math.abs((gtData[i][j] - data[i][j]) / gtData[i][j]);
						relError += e;
						relErrorNoZero += e;
					}
				}
				double me = Math.abs(gtData[i][j] - data[i][j]);
				double tmp = Math.min(gtData[i][j], data[i][j]);
				if (tmp == 0) {
					modiRelNumZero++;
					modifiedRelError += me;
				} else {
					if (tmp > 1.1102230246251565E-10) {
						me = me / tmp;
						modifiedRelError += me;
						modifiedRelErrorNoZero += me;
					}

				}
			}
		}

		int cellNum = gt.numLonBucket * gt.numLatBucket;

		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
		System.out.println("cellNum = " + cellNum);

		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total relative error = " + relError + ", avg relative error = " + (relError / cellNum);

		System.out.println(evaluation);
		evaluation = "total abs error = " + absError + ", avg abs error = " + (absError / cellNum);

		System.out.println(evaluation);

		System.out.println("--------");
		return (relError / cellNum);

	}

	public static String dataArrayEvaluation(double[][] gtData, double[][] data) {

		double relError = 0.0;

		double totalGT = 0;
		double totalData = 0;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
//				System.out.println(
//						"data[i][j] = " + data[i][j] + ", gtData[i][j] = " + gtData[i][j] + ", relError = " + relError);
				totalGT += gtData[i][j];
				totalData += data[i][j];
				if (gtData[i][j] == 0 && Math.abs(data[i][j]) > 1.1102230246251565E-10) {
					relError += Math.abs(data[i][j]);
//					System.out.println("wrong - " + data[i][j]);
				} else if (gtData[i][j] != 0) {
					if (Math.abs((gtData[i][j] - data[i][j])) > 1.1102230246251565E-10) {
						relError += Math.abs((gtData[i][j] - data[i][j]) / gtData[i][j]);
					}

				}
			}
		}

		int cellNum = gtData.length * gtData[0].length;

		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
		System.out.println("cellNum = " + cellNum);

		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total relative error = " + relError + ", avg relative error = " + (relError / cellNum);

		System.out.println(evaluation);

		System.out.println("--------");
		return relError + "," + (relError / cellNum);

	}

	public static SimpleSpatialHistogramOpt loadHistBySynoFile(List<String> lines, double minLon, double maxLon,
			double minLat, double maxLat) {

		String[] histInfo = lines.get(0).split(",");
		double sd = Double.parseDouble(histInfo[2]);

//		String[] tailInfo = lines.get(lines.size() - 1).split(";");
//		String[] rangeInfo = tailInfo[0].split(",");
//		double minLon = Double.parseDouble(rangeInfo[0]);
//		double minLat = Double.parseDouble(rangeInfo[2]);
//		double maxLon = Double.parseDouble(rangeInfo[1]);
//		double maxLat = Double.parseDouble(rangeInfo[3]);
//		String[] histInfo = tailInfo[2].split(",");
		int numLonBucket = Integer.parseInt(histInfo[0]);
		int numLatBucket = Integer.parseInt(histInfo[1]);

		double[][] data = new double[numLonBucket][numLatBucket];
		int idx = 0;
		for (int i = 1; i < numLonBucket + 1; i++) {
			double[] counts = new double[numLatBucket];
			String[] tmp = lines.get(i).replace("[", "").replace("]", "").split(",");
			for (int j = 0; j < tmp.length; j++) {
				counts[j] = Double.parseDouble(tmp[j]);
			}
			data[idx] = counts;
			idx++;
		}
		int cnt = 1 + numLonBucket;
		double[] lonBoundary = new double[numLonBucket + 1];
		String[] tmpLon = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLon.length; j++) {
			lonBoundary[j] = Double.parseDouble(tmpLon[j]);
		}
		cnt++;
		double[] latBoundary = new double[numLatBucket + 1];
		String[] tmpLat = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLat.length; j++) {
			latBoundary[j] = Double.parseDouble(tmpLat[j]);
		}
		cnt++;
		double[] lonFre = new double[numLonBucket + 1];
		String[] tmpLonFre = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLonFre.length; j++) {
			lonFre[j] = Double.parseDouble(tmpLonFre[j]);
		}
		cnt++;
		double[] latFre = new double[numLatBucket + 1];
		String[] tmpLatFre = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLatFre.length; j++) {
			latFre[j] = Double.parseDouble(tmpLatFre[j]);
		}

//		for (int i=0; i<data.length; i++) {
//			System.out.println(Arrays.toString(data[i]));
//		}

//		System.out.println(Arrays.toString(lonBoundary));
//		System.out.println(Arrays.toString(latBoundary));
//		System.out.println(Arrays.toString(lonFre));
//		System.out.println(Arrays.toString(latFre));

		SimpleSpatialHistogramOpt hist = new SimpleSpatialHistogramOpt(minLon, minLat, maxLon, maxLat, numLonBucket,
				numLatBucket, data, lonBoundary, latBoundary, lonFre, latFre, sd);

		return hist;

	}

	public static GeometricHistogramOpt loadGHBySynoFile(List<String> lines, double minLon, double maxLon,
			double minLat, double maxLat) {

		String[] histInfo = lines.get(0).split(",");
		double sd = Double.parseDouble(histInfo[2]);

		int numLonBucket = Integer.parseInt(histInfo[0]);
		int numLatBucket = Integer.parseInt(histInfo[1]);

//		double[][] data = new double[numLonBucket][numLatBucket];

		double[][] count = new double[numLonBucket][numLatBucket];
		double[][] sumRatioArea = new double[numLonBucket][numLatBucket];
		double[][] sumRatioH = new double[numLonBucket][numLatBucket];
		double[][] sumRatioV = new double[numLonBucket][numLatBucket];
		// load count
		int idx = 0;
		for (int i = 1; i < numLonBucket + 1; i++) {
			double[] counts = new double[numLatBucket];
			String[] tmp = lines.get(i).replace("[", "").replace("]", "").split(",");
			for (int j = 0; j < tmp.length; j++) {
				counts[j] = Double.parseDouble(tmp[j]);
			}
			count[idx] = counts;
			idx++;
		}
//		System.out.println(Arrays.toString(count[0]));
//		System.out.println(Arrays.toString(count[numLonBucket-1]));
		// load sumRatioArea
		int cnt = numLonBucket;
		idx = 0;
		for (int i = 1; i < numLonBucket + 1; i++) {
			double[] sumRatioAreas = new double[numLatBucket];
			String[] tmp = lines.get(i + cnt).replace("[", "").replace("]", "").split(",");
			for (int j = 0; j < tmp.length; j++) {
				sumRatioAreas[j] = Double.parseDouble(tmp[j]);
			}
			sumRatioArea[idx] = sumRatioAreas;
			idx++;
		}
//		System.out.println(Arrays.toString(sumRatioArea[0]));
//		System.out.println(Arrays.toString(sumRatioArea[numLonBucket-1]));
		// load sumRatioH
		cnt += numLonBucket;
		idx = 0;
		for (int i = 1; i < numLonBucket + 1; i++) {
			double[] sumRatioHs = new double[numLatBucket];
			String[] tmp = lines.get(i + cnt).replace("[", "").replace("]", "").split(",");
			for (int j = 0; j < tmp.length; j++) {
				sumRatioHs[j] = Double.parseDouble(tmp[j]);
			}
			sumRatioH[idx] = sumRatioHs;
			idx++;
		}
//		System.out.println(Arrays.toString(sumRatioH[0]));
//		System.out.println(Arrays.toString(sumRatioH[numLonBucket-1]));
		// load sumRatioV
		cnt += numLonBucket;
		idx = 0;
		for (int i = 1; i < numLonBucket + 1; i++) {
			double[] sumRatioVs = new double[numLatBucket];
			String[] tmp = lines.get(i + cnt).replace("[", "").replace("]", "").split(",");
			for (int j = 0; j < tmp.length; j++) {
				sumRatioVs[j] = Double.parseDouble(tmp[j]);
			}
			sumRatioV[idx] = sumRatioVs;
			idx++;
		}
//		System.out.println(Arrays.toString(sumRatioV[0]));
//		System.out.println(Arrays.toString(sumRatioV[numLonBucket-1]));
		cnt += numLonBucket + 1;
		double[] lonBoundary = new double[numLonBucket + 1];
		String[] tmpLon = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLon.length; j++) {
			lonBoundary[j] = Double.parseDouble(tmpLon[j]);
		}
//		System.out.println(Arrays.toString(lonBoundary));
		cnt++;
		double[] latBoundary = new double[numLatBucket + 1];
		String[] tmpLat = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLat.length; j++) {
			latBoundary[j] = Double.parseDouble(tmpLat[j]);
		}
//		System.out.println(Arrays.toString(latBoundary));
		cnt++;
		double[] lonFre = new double[numLonBucket + 1];
		String[] tmpLonFre = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLonFre.length; j++) {
			lonFre[j] = Double.parseDouble(tmpLonFre[j]);
		}
//		System.out.println(Arrays.toString(lonFre));
		cnt++;
		double[] latFre = new double[numLatBucket + 1];
		String[] tmpLatFre = lines.get(cnt).replace("[", "").replace("]", "").split(",");
		for (int j = 0; j < tmpLatFre.length; j++) {
			latFre[j] = Double.parseDouble(tmpLatFre[j]);
		}
//		System.out.println(Arrays.toString(lonFre));

//		for (int i=0; i<data.length; i++) {
//			System.out.println(Arrays.toString(data[i]));
//		}

//		System.out.println(Arrays.toString(lonBoundary));
//		System.out.println(Arrays.toString(latBoundary));
//		System.out.println(Arrays.toString(lonFre));
//		System.out.println(Arrays.toString(latFre));

//		public GeometricHistogramOpt(double minLon, double minLat, double maxLon, double maxLat, int numLonBucket,
//				int numLatBucket, double[][] count, double[][] sumRatioArea, double[][] sumRatioH, 
//				double[][] sumRatioV,
//				double[] lons, double[] lats, double[] lonFre, double[] latFre,
//				double sd) 

		GeometricHistogramOpt hist = new GeometricHistogramOpt(minLon, minLat, maxLon, maxLat, numLonBucket,
				numLatBucket, count, sumRatioArea, sumRatioH, sumRatioV, lonBoundary, latBoundary, lonFre, latFre, sd);

		return hist;

	}

	public static SimpleSpatialHistogramOpt loadSimpleSpatialHistogramOpt(String path, int lon, int lat) {

		SimpleSpatialHistogramOpt histogram = null;
		File file = new File(path);

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String[] header = reader.readLine().split(",");
			double minLon = 0;
			double maxLon = 0;
			double minLat = 0;
			double maxLat = 0;
			if (header.length > 2) {
				minLon = Double.parseDouble(header[0].split(":")[1].trim());
				maxLon = Double.parseDouble(header[1].split(":")[1].trim());
				minLat = Double.parseDouble(header[2].split(":")[1].trim());
				maxLat = Double.parseDouble(header[3].split(":")[1].trim());
			} else {
				minLon = Double.parseDouble(header[0]);
				maxLon = Double.parseDouble(header[1]);
				minLat = Double.parseDouble(header[0]);
				maxLat = Double.parseDouble(header[1]);
			}
			histogram = new SimpleSpatialHistogramOpt(minLon, minLat, maxLon, maxLat, lon, lat);
			String line = reader.readLine();
			while (line != null) {
				String[] tem = line.split(",");

//					histogram.addRecordNonUnitform(Double.parseDouble(tem[0]), Double.parseDouble(tem[1]));
				histogram.addRecord(Double.parseDouble(tem[0]), Double.parseDouble(tem[1]));

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

		return histogram;
	}
	
	public static String QueryDrivenError(ArrayList<double[]> queries, SimpleSpatialHistogramOpt mergeHist) {
		double score = 0;
		for (int qId = 0; qId < queries.size(); qId++) {
			double[] query = queries.get(qId);
			double GT = query[4];
			double ans = mergeHist.RangeQuery(query[0], query[1], query[2], query[3]);
			score = Math.abs(GT-ans);
		}
		return String.format("%.1f", score);
	}

	public static double AlignmentErrorUpdate(double scaleFactor, double varepsilon,
			SimpleSpatialHistogramOpt[] sourcesHist, SimpleSpatialHistogramOpt mergeHist, int scoreFunction,
			double weightFactor, boolean doNorm) {
		ArrayList<double[]> sourcesLon = new ArrayList<double[]>();
		ArrayList<double[]> sourcesLat = new ArrayList<double[]>();
		ArrayList<double[][]> sourcesData = new ArrayList<double[][]>();
		for (int i = 0; i < sourcesHist.length; i++) {
			sourcesLon.add(sourcesHist[i].getLonBoundary());
			sourcesLat.add(sourcesHist[i].getLatBoundary());
			sourcesData.add(sourcesHist[i].getData());
		}

		BetaDistribution[] betaDists = ComputeBetaDists(sourcesLon, sourcesLat, sourcesData, scaleFactor, varepsilon);

		double[] mergeLon = mergeHist.getLonBoundary();
		double[] mergeLat = mergeHist.getLatBoundary();

		double[] alignmentError = MergeToSrcUpdate(betaDists, mergeLon, mergeLat, sourcesHist, mergeHist, doNorm);
		int totalNumCell = mergeHist.getData().length * mergeHist.getData()[0].length;
		System.out.println("---Alignment Error---");
		System.out.println("Total numOfCell = " + totalNumCell);
		if (doNorm) {
			System.out.println(
					"***enlarged val***, normed avg EstRatio = " + String.format("%.3f", alignmentError[0] * 1E15)
							+ ", ***enlarged val***, normed avg DensityDiff = "
							+ String.format("%.3f", alignmentError[1] * 1E15));
		} else {
			System.out.println(
					"total avg EstRatio = " + alignmentError[0] + ", total avg DensityDiff = " + alignmentError[1]);
		}
		double score = 0;
		if (scoreFunction == 0) {
			score = alignmentError[0];
		} else if (scoreFunction == 1) {
			score = alignmentError[1];
		} else if (scoreFunction == 2) {
			score = weightFactor * alignmentError[0] + (1 - weightFactor) * alignmentError[1];
		}
		
		System.out.println("weightFactor = " + weightFactor);
		if (score > 0.001) {
			System.out.println("align func-" + scoreFunction + ", score = " + String.format("%.3f", score));
		} else {
			score = score * 1E15;
			System.out.println(
					"***enlarged val***, align func-" + scoreFunction + ", score = " + String.format("%.3f", score));
		}
		return score;
	}

	public static double AlignmentError(double scaleFactor, double varepsilon, SimpleSpatialHistogramOpt[] sourcesHist,
			SimpleSpatialHistogramOpt mergeHist) {
		ArrayList<double[]> sourcesLon = new ArrayList<double[]>();
		ArrayList<double[]> sourcesLat = new ArrayList<double[]>();
		ArrayList<double[][]> sourcesData = new ArrayList<double[][]>();
		for (int i = 0; i < sourcesHist.length; i++) {
			sourcesLon.add(sourcesHist[i].getLonBoundary());
			sourcesLat.add(sourcesHist[i].getLatBoundary());
			sourcesData.add(sourcesHist[i].getData());
		}

		BetaDistribution[] betaDists = ComputeBetaDists(sourcesLon, sourcesLat, sourcesData, scaleFactor, varepsilon);

		double[] mergeLon = mergeHist.getLonBoundary();
		double[] mergeLat = mergeHist.getLatBoundary();
		double[][] mergeData = mergeHist.getData();
		BetaDistribution mergeBetaDist = ComputeBetaDist(mergeData, mergeLon, mergeLat, scaleFactor, varepsilon);

		double[] alignmentError = MergeToSrc(betaDists, mergeBetaDist, mergeLon, mergeLat, mergeData, sourcesHist,
				mergeHist);
		int totalNumCell = mergeHist.getData().length * mergeHist.getData()[0].length;
		System.out.println("---Alignment Error---");
		System.out.println("Total numOfCell = " + totalNumCell);
//		System.out.println("TotalEst Error = " + alignmentError[0] + ", TotalAns Error = " + alignmentError[1]
//				+ ", TotalEst Error By Merge = " + alignmentError[2]);
//		System.out.println("total obj error = " + (alignmentError[0] + alignmentError[1]));
		System.out.println("AvgEst Error = " + alignmentError[0] / totalNumCell + ", AvgAns Error = "
				+ alignmentError[1] / totalNumCell + ", AvgEst Error By Merge = " + alignmentError[2] / totalNumCell);
		System.out.println("avg x+y error = " + (alignmentError[0] + alignmentError[1]) / totalNumCell);
		return alignmentError[0] + alignmentError[1];
	}

	public static double AlignmentError1DHist(double scaleFactor, double varepsilon, SimpleHistogramOpt[] sourcesHist,
			SimpleHistogramOpt mergeHist) {
		ArrayList<double[]> sourcesBoundaries = new ArrayList<double[]>();
		ArrayList<double[]> sourcesData = new ArrayList<double[]>();
		for (int i = 0; i < sourcesHist.length; i++) {
			sourcesBoundaries.add(sourcesHist[i].getBoundary());
			sourcesData.add(sourcesHist[i].getData());
		}

		BetaDistribution[] betaDists = ComputeBetaDists1DHist(sourcesBoundaries, sourcesData, scaleFactor, varepsilon);

		double[] mergeBoundaries = mergeHist.getBoundary();
		double[] mergeData = mergeHist.getData();
		BetaDistribution mergeBetaDist = ComputeBetaDist1DHist(mergeData, mergeBoundaries, scaleFactor, varepsilon);

		double[] alignmentError = MergeToSrc1DHist(betaDists, mergeBetaDist, mergeBoundaries, mergeData, sourcesHist,
				mergeHist);
		System.out.println("---Alignment Error---");
		System.out.println("TotalEst Error = " + alignmentError[0] + ", TotalAns Error = " + alignmentError[1]
				+ ", TotalEst Error By Merge = " + alignmentError[2]);
		System.out.println("total obj error = " + (alignmentError[0] + alignmentError[1]));
		return alignmentError[0] + alignmentError[1];
	}

	public static double[] MergeToSrc1DHist(BetaDistribution[] betaDists, BetaDistribution mergeBetaDist,
			double[] mergeBoundaries, double[] mergeData, SimpleHistogramOpt[] sourcesHist,
			SimpleHistogramOpt mergeHist) {
		double totalErrorEst = 0.0;
		for (int i = 0; i < mergeBoundaries.length - 1; i++) {
			double min = mergeBoundaries[i];
			double max = mergeBoundaries[i + 1];
			for (int srcId = 0; srcId < sourcesHist.length; srcId++) {
				double[] answers = sourcesHist[srcId].RangeQueryWithRatio(betaDists[srcId], min, max);
//				answers[0] = ans;
//				answers[1] = errors;
//				answers[2] = errorRatio;
//				// errors
//				totalErrorEst += answers[1];
				// errorRatio
				totalErrorEst += answers[2];
			}
		}

		double totalAbsError = 0.0;
		double totalErrorEstFromMerge = 0.0;
		for (int srcId = 0; srcId < sourcesHist.length; srcId++) {
			double[] srcBoundaries = sourcesHist[srcId].getBoundary();
			double[] srcData = sourcesHist[srcId].getData();
			for (int i = 0; i < srcBoundaries.length - 1; i++) {
				double min = srcBoundaries[i];
				double max = srcBoundaries[i + 1];
				double[] answers = mergeHist.RangeQueryWithRatio(mergeBetaDist, min, max);
				totalAbsError += Math.abs(srcData[i] - answers[0]);
//				// errors
//				totalErrorEstFromMerge += answers[1];
				// errorRatio
				totalErrorEstFromMerge += answers[2];
			}
		}
		double[] error = new double[3];
		error[0] = totalErrorEst;
		error[1] = totalAbsError;
		error[2] = totalErrorEstFromMerge;
		return error;
	}

	public static double[] MergeToSrcUpdate(BetaDistribution[] betaDists, double[] mergeLon, double[] mergeLat,
			SimpleSpatialHistogramOpt[] sourcesHist, SimpleSpatialHistogramOpt mergeHist, boolean doNorm) {
		double totalErrorEst = 0.0;
		ArrayList<Double> estErrors = new ArrayList<Double>();

		double totalDensityDiff = 0.0;
		ArrayList<Double> densityDiffs = new ArrayList<Double>();

		for (int i = 0; i < mergeLon.length - 1; i++) {
			double minLon = mergeLon[i];
			double maxLon = mergeLon[i + 1];
			for (int j = 0; j < mergeLat.length - 1; j++) {
				double minLat = mergeLat[j];
				double maxLat = mergeLat[j + 1];
				ArrayList<Double> allSrcDensities = new ArrayList<Double>();
//				ArrayList<Double> curEstRatios = new ArrayList<Double>();
				double errSrcToTar = 0;
				double curTotalOverlapRatio = 0.0;
				for (int srcId = 0; srcId < sourcesHist.length; srcId++) {
//					answers[0] = ans;
//					answers[2] = totalErrorRatio;
//					answers[3] = totalRatios;
					double[] ans = sourcesHist[srcId].RangeQueryWithRatio(betaDists[srcId], minLon, maxLon, minLat,
							maxLat);
					if (ans.length > 3 && ans[3] > 0) {
//						curEstRatios.add(ans[2] * ans[3]);
						errSrcToTar += (ans[2] * ans[3]);
						curTotalOverlapRatio += ans[3];
						ArrayList<Double> densities = sourcesHist[srcId].RangeQueryGetDensity(minLon, maxLon, minLat,
								maxLat);
						allSrcDensities.addAll(densities);
					}
				}
//				double errSrcToTar = 0;
//				for (int m = 0; m < curEstRatios.size(); m++) {
//					errSrcToTar += curEstRatios.get(m) / curTotalOverlapRatio;
//				}
				errSrcToTar = (errSrcToTar/ curTotalOverlapRatio);
				estErrors.add(errSrcToTar);
				totalErrorEst += errSrcToTar;
//				System.out.println(allSrcDensities);
				double densityDiff = 0;
				for (int m = 0; m < allSrcDensities.size(); m++) {
					for (int n = m + 1; n < allSrcDensities.size(); n++) {
						densityDiff = Math.abs(allSrcDensities.get(m) - allSrcDensities.get(n));
					}
				}
				if (allSrcDensities.size() > 1) {
					totalDensityDiff += densityDiff;
					densityDiffs.add(densityDiff);
				}
			}
		}

		if (doNorm) {
			double meanErrorEst = totalErrorEst / estErrors.size();
			double sdErrorEst = UtilsFunction.ComputeStandardDeviation(estErrors, true, meanErrorEst);
			double normedErrorEst = 0;
			for (int i = 0; i < estErrors.size(); i++) {
				double estError = estErrors.get(i);
				normedErrorEst += (estError - meanErrorEst) / sdErrorEst;
			}

			double meanDensityDiff = totalDensityDiff / densityDiffs.size();
			double sdDensityDiff = UtilsFunction.ComputeStandardDeviation(densityDiffs, true, meanDensityDiff);
			double normedDensityDiff = 0;
			for (int i = 0; i < densityDiffs.size(); i++) {
				double densityDiff = densityDiffs.get(i);
				normedDensityDiff += (densityDiff - meanDensityDiff) / sdDensityDiff;
			}
			double[] error = new double[2];
			error[0] = Math.abs(normedErrorEst / (mergeLon.length - 1) / (mergeLat.length - 1));
			error[1] = Math.abs(normedDensityDiff / (mergeLon.length - 1) / (mergeLat.length - 1));
			return error;
		} else {
			double[] error = new double[2];
			error[0] = Math.abs(totalErrorEst / (mergeLon.length - 1) / (mergeLat.length - 1));
			error[1] = Math.abs(totalDensityDiff / (mergeLon.length - 1) / (mergeLat.length - 1));
			return error;
		}
	}

	public static double[] MergeToSrc(BetaDistribution[] betaDists, BetaDistribution mergeBetaDist, double[] mergeLon,
			double[] mergeLat, double[][] mergeData, SimpleSpatialHistogramOpt[] sourcesHist,
			SimpleSpatialHistogramOpt mergeHist) {
		double totalErrorEst = 0.0;
		for (int i = 0; i < mergeLon.length - 1; i++) {
			double minLon = mergeLon[i];
			double maxLon = mergeLon[i + 1];
			for (int j = 0; j < mergeLat.length - 1; j++) {
				double minLat = mergeLat[j];
				double maxLat = mergeLat[j + 1];
				for (int srcId = 0; srcId < sourcesHist.length; srcId++) {
					double[] answers = sourcesHist[srcId].RangeQueryWithRatio(betaDists[srcId], minLon, maxLon, minLat,
							maxLat);
					totalErrorEst += answers[2];
				}
			}
		}

		double totalAbsError = 0.0;
		double totalErrorEstFromMerge = 0.0;
		for (int srcId = 0; srcId < sourcesHist.length; srcId++) {
			double[] srcLon = sourcesHist[srcId].getLonBoundary();
			double[] srcLat = sourcesHist[srcId].getLatBoundary();
			double[][] srcData = sourcesHist[srcId].getData();
			for (int i = 0; i < srcLon.length - 1; i++) {
				double minLon = srcLon[i];
				double maxLon = srcLon[i + 1];
				for (int j = 0; j < srcLat.length - 1; j++) {
					double minLat = srcLat[j];
					double maxLat = srcLat[j + 1];
					double[] answers = mergeHist.RangeQueryWithRatio(mergeBetaDist, minLon, maxLon, minLat, maxLat);
					totalAbsError += Math.abs(srcData[i][j] - answers[0]);
					totalErrorEstFromMerge += answers[2];
//					System.out.println(srcData[i][j] + " - " + answers[0]);
				}
			}
		}
		double[] error = new double[3];
		error[0] = totalErrorEst;
		error[1] = totalErrorEstFromMerge;
		error[2] = totalAbsError;
		return error;
	}

	public static BetaDistribution ComputeBetaDist(double[][] data, double[] boundariesLon, double[] boundariesLat,
			double scaleFactor, double varepsilon) {
		double[] densities = new double[data.length * data[0].length];
		for (int j = 0; j < boundariesLon.length - 1; j++) {
			double lonUnit = boundariesLon[j + 1] - boundariesLon[j];
			for (int k = 0; k < boundariesLat.length - 1; k++) {
				double latUnit = boundariesLat[k + 1] - boundariesLat[k];
				densities[j * data.length + k] = data[j][k] / lonUnit / latUnit;
//				System.out.println(lonUnit + "-" + latUnit);
			}

		}
//		System.out.println("call median from ComputeBetaDist");
		double median = UtilsFunction.CalculateMedian(densities);
//		System.out.println(Arrays.toString(densities));
//		System.out.println(densities[densities.length/2]);
		double MAD = UtilsFunction.computeMADByLibrary(densities);
//		double MAD = UtilsFunction.computeMAD(densities);
		double DK = MAD / (median + varepsilon);
		double betaParameters = scaleFactor / (DK + varepsilon) + 1;
//		System.out.println("MAD=" + String.format("%.3f", MAD) + ", DK=" + DK + ", shaped paramters="
//				+ String.format("%.3f", betaParameters));
		BetaDistribution betaDist = new BetaDistribution(betaParameters, betaParameters);

		return betaDist;
	}

	public static BetaDistribution[] ComputeBetaDists(ArrayList<double[]> sourcesLon, ArrayList<double[]> sourcesLat,
			ArrayList<double[][]> sourcesData, double scaleFactor, double varepsilon) {
		double[] dataSkewness = new double[sourcesData.size()];
		double[] srcBetaParams = new double[sourcesData.size()];
		BetaDistribution[] betaDists = new BetaDistribution[sourcesData.size()];

		// pre-processing - 1: compute density
		for (int i = 0; i < sourcesData.size(); i++) {
			double[][] data = sourcesData.get(i);
			double[] boundariesLon = sourcesLon.get(i);
			double[] boundariesLat = sourcesLat.get(i);
			double[] densities = new double[data.length * data[0].length];
			for (int j = 0; j < boundariesLon.length - 1; j++) {
				double lonUnit = boundariesLon[j + 1] - boundariesLon[j];
				for (int k = 0; k < boundariesLat.length - 1; k++) {
					double latUnit = boundariesLat[k + 1] - boundariesLat[k];
					densities[j * data.length + k] = data[j][k] / lonUnit / latUnit;
//					System.out.println(lonUnit + "-" + latUnit);
				}

			}
//			System.out.println("density = " + Arrays.toString(densities));
			double median = UtilsFunction.CalculateMedian(densities);
			double MAD = UtilsFunction.computeMADByLibrary(densities);
			double DK = MAD / (median + varepsilon);
			double betaParameters = scaleFactor / (DK + varepsilon) + 1;
			BetaDistribution betaDist = new BetaDistribution(betaParameters, betaParameters);
			dataSkewness[i] = DK;
			srcBetaParams[i] = betaParameters;
			betaDists[i] = betaDist;
		}
//		System.out.println("data skewness = " + Arrays.toString(dataSkewness));
//		System.out.println("beta parameters = " + Arrays.toString(srcBetaParams));

		return betaDists;
	}

	public static BetaDistribution ComputeBetaDist1DHist(double[] data, double[] boundaries, double scaleFactor,
			double varepsilon) {
		double[] densities = new double[data.length];
		for (int j = 0; j < boundaries.length - 1; j++) {
			densities[j] = data[j] / boundaries[j] / boundaries[j + 1];
		}
		double median = UtilsFunction.CalculateMedian(densities);
		double MAD = UtilsFunction.computeMADByLibrary(densities);
		double DK = MAD / (median + varepsilon);
		double betaParameters = scaleFactor / (DK + varepsilon) + 1;
		BetaDistribution betaDist = new BetaDistribution(betaParameters, betaParameters);
		System.out.println("MAD = " + MAD + ", DK = " + DK + ", scaleFactor = " + scaleFactor + ", betaParameters = "
				+ betaParameters);
		return betaDist;
	}

	public static BetaDistribution[] ComputeBetaDists1DHist(ArrayList<double[]> sourcesBoundaries,
			ArrayList<double[]> sourcesData, double scaleFactor, double varepsilon) {
		double[] dataSkewness = new double[sourcesData.size()];
		double[] srcBetaParams = new double[sourcesData.size()];
		BetaDistribution[] betaDists = new BetaDistribution[sourcesData.size()];

		// pre-processing - 1: compute density
		for (int i = 0; i < sourcesData.size(); i++) {
			double[] data = sourcesData.get(i);
			double[] boundaries = sourcesBoundaries.get(i);
			double[] densities = new double[data.length];
			for (int j = 0; j < boundaries.length - 1; j++) {
				densities[j] = data[j] / boundaries[j] / boundaries[j + 1];
			}
			double median = UtilsFunction.CalculateMedian(densities);
			double MAD = UtilsFunction.computeMADByLibrary(densities);
			double DK = MAD / (median + varepsilon);
			double betaParameters = scaleFactor / (DK + varepsilon) + 1;
			BetaDistribution betaDist = new BetaDistribution(betaParameters, betaParameters);
			dataSkewness[i] = DK;
			srcBetaParams[i] = betaParameters;
			betaDists[i] = betaDist;
		}
		System.out.println("data skewness = " + Arrays.toString(dataSkewness));
		System.out.println("beta parameters = " + Arrays.toString(srcBetaParams));

		return betaDists;
	}

}
