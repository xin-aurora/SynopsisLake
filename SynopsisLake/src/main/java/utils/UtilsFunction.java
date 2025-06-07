package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.special.Beta;
import org.apache.commons.math3.stat.descriptive.rank.Median;

public class UtilsFunction {

	public static boolean isOverlapPointInterval(double tMin, double tMax, double x) {

		if (x < tMin || x > tMax) {
			return false;
		}

		return true;

	}

	public static boolean isOverlapPointCell(double tMinLon, double tMinLat, double tMaxLon, double tMaxLat, double lon,
			double lat) {

		if (tMaxLon < lon || lon < tMinLon || tMaxLat < lat || lat < tMinLat) {
			return false;
		}

		return true;

	}

	public static boolean isOverlapInterval(double tMin, double tMax, double sMin, double sMax) {

		if (tMax <= sMin || sMax <= tMin) {
			return false;
		} else {
			return true;
		}

	}

	public static boolean isOverlapCellCell(double tMinLon, double tMinLat, double tMaxLon, double tMaxLat,
			double sMinLon, double sMinLat, double sMaxLon, double sMaxLat) {

		if (tMaxLon < sMinLon || sMaxLon < tMinLon || tMaxLat < sMinLat || sMaxLat < tMinLat) {
			return false;
		} else {
			double minLonInt = Math.max(tMinLon, sMinLon);
			double maxLonInt = Math.min(tMaxLon, sMaxLon);
			double minLatInt = Math.max(tMinLat, sMinLat);
			double maxLatInt = Math.min(tMaxLat, sMaxLat);

			// compute values
			double w = maxLonInt - minLonInt;
			double h = maxLatInt - minLatInt;
			if (w < 1.1102230246251565E-10 || h < 1.1102230246251565E-10) {
				return false;
			}

			return true;
		}

	}

	public static double SpearmansCorrelationCoefficient(ArrayList<Double> firstData, ArrayList<Double> secondData) {

		int n = firstData.size();

		ArrayList<Double> firstRankData = new ArrayList<Double>(firstData);
		Collections.sort(firstRankData);

		ArrayList<Double> secondRankData = new ArrayList<Double>(secondData);
		Collections.sort(secondRankData);

		System.out.println(firstRankData);
		System.out.println(firstData);

		System.out.println(secondRankData);
		System.out.println(secondData);

		int[] firstRank = new int[n];
		int[] secondRank = new int[n];

		double firstAvg = 0;
		double secondAvg = 0;
		for (int i = 0; i < n; i++) {
			firstRank[i] = firstRankData.indexOf(firstData.get(i));
			secondRank[i] = secondRankData.indexOf(secondData.get(i));

			firstAvg += firstRank[i];
			secondAvg += secondRank[i];
		}

		System.out.println();
		System.out.println(Arrays.toString(firstRank));
		System.out.println(Arrays.toString(secondRank));

//		// distinct
//		double r = 0;
//		double sumDPow = 0;
//		for (int i=0; i<n; i++) {
//			sumDPow += Math.pow((firstRank[i] - secondRank[i]), 2);
//		}
//		
//		r = 1 - (6 * sumDPow) / firstRank.length / (firstRank.length * firstRank.length - 1);

		// compute avg and sd
		firstAvg = firstAvg / n;
		secondAvg = secondAvg / n;
		double firstSD = 0;
		double secondSD = 0;
		double r = 0;
		double cov = 0;
		for (int i = 0; i < n; i++) {
			firstSD += Math.pow((firstRank[i] - firstAvg), 2);
			secondSD += Math.pow((secondRank[i] - secondAvg), 2);

			cov += (firstRank[i] - firstAvg) * (secondRank[i] - secondAvg);
		}

		r = cov / Math.sqrt(firstSD * secondSD);

		System.out.println("r = " + r);

		return r;
	}

	public static void SpaceOverhead() {
		// wildfire
		double noSyno = 0.800890512;
		double ch = 0.879588756;
		double wave = 0.821763210;
		// syn
//		noSyno = 0.852064194;
//		ch = 0.995677335;
//		wave = 0.897041968;
//		// osm
//		noSyno = 5.264850527;
//		ch = 5.962438574;
//		wave = 5.433682165;

		System.out.println("ch overhead = " + (ch - noSyno));
		System.out.println("wave overhead = " + (wave - noSyno));

		// lake
		noSyno = 0.426705316;
		double gh = 0.628279670;
		// park
		noSyno = 0.544700653;
		gh = 0.768603949;
		System.out.println("gh overhead = " + (gh - noSyno));
	}

	public static double[][] loadSynopsisPrecsion(int numLine) {

		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/"
				+ "sketches/exp/figure/synopsis_precision/";
		String filePath = folder + "lake_count_error_score.txt";

		double[][] error = new double[numLine][2];

		File file = new File(filePath);
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line = reader.readLine();

			int cnt = 0;
			while (line != null) {

				if (!line.startsWith("#")) {
					String[] tmp = line.split("\t");
//					System.out.println(Arrays.toString(tmp));
					double[] errorScore = new double[2];
					errorScore[0] = Double.parseDouble(tmp[0]);
					errorScore[1] = Double.parseDouble(tmp[1].trim());
					error[cnt] = errorScore;
					cnt++;
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

		return error;
	}

	public static void Find1DRange(String filePath, String regex) {
		double min = Double.MAX_VALUE;
		double max = -Double.MAX_VALUE;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();

			while (line != null) {

				String[] tem = line.split(regex);
				double x = Double.parseDouble(tem[0]);
				min = Math.min(min, x);
				max = Math.max(max, x);
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
		System.out.println("min = " + min + ", max = " + max);
	}

	public static double ComputeMean(ArrayList<Double> data) {

		int n = data.size();

		double sum = 0.0;

		// Compute the sum of all elements
		for (double value : data) {
			sum += value;
		}

		// Calculate the mean
		return sum / n;
	}

	public static double ComputeMean(double[] data) {

		int n = data.length;

		double sum = 0.0;

		// Compute the sum of all elements
		for (double value : data) {
			sum += value;
		}

		// Calculate the mean
		return sum / n;
	}

	public static double ComputeStandardDeviation(ArrayList<Double> data, boolean hasMean, double mean) {
		int n = data.size();

		if (!hasMean) {
			// Compute the mean
			mean = ComputeMean(data);
		}

		// Compute the variance
		double variance = 0.0;
		for (double value : data) {
			variance += Math.pow(value - mean, 2);
		}
		variance /= n; // Use n-1 for sample standard deviation (Bessel's correction)

		// Compute the standard deviation
		return Math.sqrt(variance);
	}

	public static double ComputeStandardDeviation(double[] data) {
		int n = data.length;

		// Compute the mean
		double mean = ComputeMean(data);

		// Compute the variance
		double variance = 0.0;
		for (double value : data) {
			variance += Math.pow(value - mean, 2);
		}
		variance /= n; // Use n-1 for sample standard deviation (Bessel's correction)

		// Compute the standard deviation
		return Math.sqrt(variance);
	}

	public static double ComputeVariance(double mean, double[] data, boolean isSample) {

		// Compute the variance
		double sumSquaredDifferences = 0.0;
		for (double value : data) {
			sumSquaredDifferences += Math.pow(value - mean, 2);
		}

		if (isSample) {
			// For sample variance, use: (data.length - 1) in the denominator
			return sumSquaredDifferences / (data.length - 1);
		} else {
			return sumSquaredDifferences / data.length; // Population variance
		}
	}

	public static double computeMADByLibrary(double[] data) {
		Median median = new Median();
		double medianValue = median.evaluate(data);

		// Calculate absolute deviations from the median
		double[] deviations = new double[data.length];
		for (int i = 0; i < data.length; i++) {
			deviations[i] = Math.abs(data[i] - medianValue);
		}

		// Calculate MAD as the median of the deviations
		return median.evaluate(deviations);
//		double MAD = median.evaluate(deviations);
	}

	public static double computeMAD(double[] data) {
		double median = CalculateMedian(data);

		// Calculate absolute deviations from the median
		double[] deviations = new double[data.length];
		for (int i = 0; i < data.length; i++) {
			deviations[i] = Math.abs(data[i] - median);
		}
//		double MADS = calculateMedian(deviations);
		return CalculateMedian(deviations);
	}

	public static double computeMAD(ArrayList<Double> data) {
		double median = CalculateMedian(data);

		// Calculate absolute deviations from the median
		double[] deviations = new double[data.size()];
		for (int i = 0; i < data.size(); i++) {
			deviations[i] = Math.abs(data.get(i) - median);
		}
//		double MADS = calculateMedian(deviations);
//		System.out.println("computeMAD");
		return CalculateMedian(deviations);
	}

	public static double CalculateMedian(double[] values) {
//		double[] dummyArray = new double[values.length];
//		dummyArray = Arrays.copyOf(values, values.length);
		double[] dummyArray = values.clone();
		Arrays.sort(dummyArray);
//		System.out.println("dummy array: " + Arrays.toString(dummyArray));
//		System.out.println("values array: " + Arrays.toString(values));
		int n = dummyArray.length;
//		System.out.println(n);
		if (n % 2 == 0) {
			return (dummyArray[n / 2 - 1] + dummyArray[n / 2]) / 2.0;
		} else {
			return dummyArray[n / 2];
		}
	}

	public static double CalculateMedian(ArrayList<Double> values) {
		ArrayList<Double> dummyList = new ArrayList<Double>(values.size());
//		System.out.println(values.size() + ", " + dummyList.size());
//		Collections.copy(dummyList, values);
		dummyList.addAll(values);
		Collections.sort(dummyList);
		int n = dummyList.size();
		if (n % 2 == 0) {
			return (dummyList.get(n / 2 - 1) + dummyList.get(n / 2)) / 2.0;
		} else {
			return dummyList.get(n / 2);
		}
	}

	public static double BetaDistPDF(double alpha, double beta, double low, double high) {
		BetaDistribution betaDist = new BetaDistribution(alpha, beta);
		double pdf = betaDist.probability(low, high);
		return pdf;
	}

	public static double BetaDistCDF(double alpha, double beta, double x) {
		BetaDistribution betaDist = new BetaDistribution(alpha, beta);
		double cdf = betaDist.cumulativeProbability(x);
		return cdf;
	}

	public static double BetaDistErrorRatio(BetaDistribution betaDist, double overlap) {
		double a = 0.5 - overlap / 2;
		double b = 0.5 + overlap / 2;
//		System.out.println(betaDist.cumulativeProbability(b));
//		System.out.println(betaDist.cumulativeProbability(a));
		double errorRatio = 1.0 - betaDist.cumulativeProbability(b) + betaDist.cumulativeProbability(a);
//		double errorRatio = betaDist.cumulativeProbability(b) - betaDist.cumulativeProbability(a);
		return errorRatio;
	}
}
