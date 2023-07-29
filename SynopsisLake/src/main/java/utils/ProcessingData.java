package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Pattern;

public class ProcessingData {

	public static void ExtractData(String inputPath, String outputPath) {
		File file = new File(inputPath);

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			FileWriter myWriter = new FileWriter(outputPath, false);
			String[] header = reader.readLine().split("\t");
//			for (int i=0; i<header.length; i++) {
//				System.out.println(i + ": " + header[i]);
//			}
			String str = header[0] + "," + header[1] + "\n";
			myWriter.write(str);
			String line = reader.readLine();
			while (line != null) {
				String[] tem = line.split("\t");

//				for (int i=0; i<tem.length; i++) {
//					System.out.println(i + ": " + tem[i]);
//				}

//				str = tem[0] + "," + tem[1] + "\n";
//				System.out.println(str);
				if (tem.length > 2) {
					str = tem[0] + "," + tem[1] + "\n";
					myWriter.write(str);
				}
//				myWriter.write(str);

				line = reader.readLine();
			}
			reader.close();
			myWriter.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void GetBoundary(String inputPath) {

		double MAX_LON = -Double.MAX_VALUE;
		double MAX_LAT = -Double.MIN_VALUE;
		double MIN_LON = Double.MAX_VALUE;
		double MIN_LAT = Double.MAX_VALUE;

		File file = new File(inputPath);

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String[] header = reader.readLine().split(",");

			String line = reader.readLine();
			while (line != null) {
				String[] tem = line.split(",");

				double x = Double.parseDouble(tem[0]);

				double y = Double.parseDouble(tem[1]);

				if (x < MIN_LON) {
					MIN_LON = x;
				}

				if (x > MAX_LON) {
					MAX_LON = x;
				}

				if (y < MIN_LAT) {
					MIN_LAT = y;
				}

				if (y > MAX_LAT) {
					MAX_LAT = y;
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

		System.out.println(
				"min lon: " + MIN_LON + ", max lon: " + MAX_LON + ", min lat: " + MIN_LAT + ", max lat: " + MAX_LAT);
	}

	public static void PartitionGenerator(String outputFolder, double minLon, double maxLon, double minLat,
			double maxLat, int lonNumPar, int latNumPar) {

		double lonUnit = (maxLon - minLon) / lonNumPar;
		double latUnit = (maxLat - minLat) / latNumPar;
		int pId = 0;
		for (int i = 0; i < lonNumPar; i++) {
			double pMinLon = minLon + i * lonUnit;
			double pMaxLon = pMinLon + lonUnit;
			for (int j = 0; j < latNumPar; j++) {

				double pMinLat = minLat + j * latUnit;
				double pMaxLat = pMinLat + latUnit;

				String pFileName = outputFolder + "/p" + pId + ".csv";
				try {
					FileWriter myWriter = new FileWriter(pFileName, false);
					String header = "minlon:" + pMinLon + ",maxlon:" + pMaxLon + ",minlat:" + pMinLat + ",maxlat:"
							+ pMaxLat + "\n";
					myWriter.write(header);
					myWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				pId++;
			}
		}
	}

	public static void FillPartition(String filePath, String parPath) {
		double minLon = 0;
		double maxLon = 0;
		double minLat = 0;
		double maxLat = 0;
		
		// load partition are
		File file = new File(parPath);
		File inputFile = new File(filePath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String[] header = reader.readLine().split(",");

			minLon = Double.parseDouble(header[0].split(":")[1]);
			maxLon = Double.parseDouble(header[1].split(":")[1]);
			minLat = Double.parseDouble(header[2].split(":")[1]);
			maxLat = Double.parseDouble(header[3].split(":")[1]);
			
			reader.close();
			
			FileWriter myWriter = new FileWriter(parPath, true);
			reader = new BufferedReader(new FileReader(inputFile));
			String line = reader.readLine(); // header
			line = reader.readLine();
			int cnt = 0;
			while (line != null) {
				String[] tem = line.split(",");

				double x = Double.parseDouble(tem[0]);

				double y = Double.parseDouble(tem[1]);

				if (UtilsFunction.isOverlapPointCell(minLon, minLat, maxLon, maxLat, x, y)) {
					myWriter.write(line);
					cnt++;
				}

				line = reader.readLine();
			}
			reader.close();
			myWriter.close();
			System.out.println("num of points = " + cnt);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/data/real-world/";
//		String inputPath = folder + "/sample/osm21_pois_sample.csv";
//		String inputPath = folder + "osm21_pois.csv";
//		String outputPath = folder + "osm21_pois_coords.csv";
		String outputPath = folder + "wildfiredb_coords.csv";

//		ExtractData(inputPath, outputPath);
//		GetBoundary(outputPath);

//		String outputFolder = folder + "osm21_pois/";
//		double minlon = -180.0;
//		double maxlon = 180.0;
//		double minlat = -90.0;
//		double maxlat = 90.0;
//		int lonNumPar = 10;
//		int latNumPar = 4;
		String outputFolder = folder + "wildfiredb/";
		double minlon = -124.66016074688953;
		double maxlon = -66.99431782299374;
		double minlat = 24.56637998036461;
		double maxlat = 48.99404003120753;
		int lonNumPar = 3;
		int latNumPar = 3;
		PartitionGenerator(outputFolder, minlon, maxlon, minlat, maxlat, lonNumPar, latNumPar);

//		String filePath = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/data/real-world/wildfiredb_coords.csv";
//		String parPath = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/data/real-world/wildfiredb/p0.csv";
//		for (int i = 0; i < args.length; i++) {
//			if (args[i].equals("-filePath")) {
//				filePath = args[++i];
//			} else if (args[i].equals("-parPath")) {
//				parPath = args[++i];
//			}
//		}
//		FillPartition(filePath, parPath);
	}

}
