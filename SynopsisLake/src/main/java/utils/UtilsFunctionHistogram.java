package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import dataStructure.SSH2DCell;
import histogram.GeometricHistogramOpt;
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
		return r;

	}

	public static void SimpleSpatialHistogramOptValidation(SimpleSpatialHistogramOpt hist,
			SimpleSpatialHistogram baseline) {

		double[][] data = hist.getData();
		SSH2DCell[][] cells = baseline.getCells();

		boolean valid = true;
		for (int i = 0; i < data.length; i++) {
			for (int j = 0; j < data[0].length; j++) {
//				System.out.println(i + ": d=" + data[i][j] + ", bs=" + cells[i][j].getVal());
				if (Math.abs(data[i][j] - cells[i][j].getVal()) > 1.1102230246251565E-10) {
					System.out.println("not valid! " + i + ": d=" + data[i][j] + ", bs=" + cells[i][j].getVal());
					valid = false;
				}
//				else {
////					System.out.println("---" + i + ": d=" + data[i][j] + ", bs=" + cells[i][j].getVal());
//				}
			}
		}

		System.out.println("valid = " + valid);

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
		return relError + "," + (relError / cellNum) + "," + relErrorNoZero + ","
				+ (relErrorNoZero / (cellNum - relNumZero)) + "," + relNumZero + "," + modifiedRelError + ","
				+ (modifiedRelError / cellNum) + "," + modifiedRelErrorNoZero + ","
				+ (modifiedRelErrorNoZero / (cellNum - modiRelNumZero)) + "," + modifiedRelErrorNoZero + "," + absError
				+ "," + (absError / cellNum);

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

		String evaluation = "total relative error = " + relErrorTotal 
				+ ", avg relative error = " + (relErrorAvgTotal / hists.length);
		System.out.println(evaluation);
		evaluation = "total abs error = " + absErrorTotal 
				+ ", avg abs error = " + (absErrorAvgTotal / hists.length);
		System.out.println(evaluation);

		return relErrorTotal + "," + (relErrorAvgTotal / hists.length) + "," 
		+ relErrorNoZeroTotal + "," + (relErrorNoZeroAvgTotal / hists.length) + ","
				+ modifiedRelErrorTotal + "," + (modifiedRelErrorAvgTotal / hists.length)
				+ "," + modifiedRelErrorNoZeroTotal + ","
				+ (modifiedRelErrorNoZeroAvgTotal / hists.length) + "," 
				+ absErrorTotal + "," + (absErrorAvgTotal / hists.length);

	}
	
	public static String GeometricHistogramOptEvaluation(GeometricHistogramOpt[] gts,
			GeometricHistogramOpt[] hists) {

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

		String evaluation = "total relative error = " + relErrorTotal 
				+ ", avg relative error = " + (relErrorAvgTotal / hists.length);
		System.out.println(evaluation);
		evaluation = "total abs error = " + absErrorTotal 
				+ ", avg abs error = " + (absErrorAvgTotal / hists.length);
		System.out.println(evaluation);

		return relErrorTotal + "," + (relErrorAvgTotal / hists.length) + "," 
		+ relErrorNoZeroTotal + "," + (relErrorNoZeroAvgTotal / hists.length) + ","
				+ modifiedRelErrorTotal + "," + (modifiedRelErrorAvgTotal / hists.length)
				+ "," + modifiedRelErrorNoZeroTotal + ","
				+ (modifiedRelErrorNoZeroAvgTotal / hists.length) + "," 
				+ absErrorTotal + "," + (absErrorAvgTotal / hists.length);

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

		System.out.println("----SimpleSpatialHistogramOpt Evaluation----");
		System.out.println("cellNum = " + cellNum);

		System.out.println("totalGT = " + totalGT + ", totalData = " + totalData);
		String evaluation = "total count relative error = " + relErrorCount + ", avg relative error = "
				+ (relErrorCount / cellNum);
		System.out.println(evaluation);
		evaluation = "total abs count error = " + absErrorCount + ", avg abs error = " + (absErrorCount / cellNum);
		System.out.println(evaluation);

		evaluation = "total area relative error = " + relErrorArea + ", avg relative error = "
				+ (relErrorArea / cellNum);
		System.out.println(evaluation);
		evaluation = "total area count error = " + absErrorArea + ", avg abs error = " + (absErrorArea / cellNum);
		System.out.println(evaluation);

		evaluation = "total RH relative error = " + relErrorRH + ", avg relative error = " + (relErrorRH / cellNum);
		System.out.println(evaluation);
		evaluation = "total RH count error = " + absErrorRH + ", avg abs error = " + (absErrorRH / cellNum);
		System.out.println(evaluation);

		evaluation = "total RV relative error = " + relErrorRV + ", avg relative error = " + (relErrorRV / cellNum);
		System.out.println(evaluation);
		evaluation = "total RV count error = " + absErrorRV + ", avg abs error = " + (absErrorRV / cellNum);
		System.out.println(evaluation);

		System.out.println("--------");
		return "";

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

}
