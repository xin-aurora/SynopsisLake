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

public class UtilsFunction {

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
		noSyno = 0.426705316 ;
		double gh = 0.628279670;
		// park
		noSyno = 0.544700653;
		gh = 0.768603949;
		System.out.println("gh overhead = " + (gh - noSyno));
	}
	
	public static double[][] loadSynopsisPrecsion(int numLine){
		
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

	public static void main(String[] args) {
		
		ArrayList<Double> firstData = new ArrayList<Double>();
		ArrayList<Double> secondData = new ArrayList<Double>();
		
		int numLine = 12;
		double[][] error = loadSynopsisPrecsion(numLine);
		for (int i=0; i<error.length; i++) {
//			System.out.println(Arrays.toString(error[i]));
			firstData.add(error[i][0]);
			secondData.add(error[i][1]);
		}

//		SpearmansCorrelationCoefficient(firstData, secondData);

		SpaceOverhead();
	}

}
