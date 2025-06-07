package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;
import java.util.stream.Stream;

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

	public static void GeneratedPartitionedData(int numOfLonPar, int numOfLatPar, String folder, String filePath,
			int totalNumItem, int totalNumItemMin) {

		Random rand = new Random();
		double lonStart = 0.0;
		double latStart = 0.0;
		// rangeMin + (rangeMax - rangeMin) * rand
//		double lonGap = 5 + (15 - 5) * rand.nextDouble();
//		double lonGap = 0.0;
//		double latGap = 0.0;
		int numOfPar = numOfLonPar * numOfLatPar;
		try {
			FileWriter writer = new FileWriter(folder + numOfPar + "clusters500K.csv");
			for (int lonId = 0; lonId < numOfLonPar; lonId++) {
				for (int latId = 0; latId < numOfLatPar; latId++) {
					// generate range
					double lonLen = 30 + (60 - 30) * rand.nextDouble();
					double latLen = 30 + (60 - 30) * rand.nextDouble();

					double lonEnd = lonLen + lonStart;
					double latEnd = latLen + latStart;

					// process the data
					BufferedReader reader = new BufferedReader(new FileReader(filePath));
//					String[] header = reader.readLine().split(",");
//					int numOfItems = rand.nextInt((totalNumItem - totalNumItemMin) + 1) + totalNumItemMin;
					for (int i = 0; i < totalNumItem; i++) {
						String line = reader.readLine();
						String[] tem = line.split(",");
						double x = Double.parseDouble(tem[0]);
						double y = Double.parseDouble(tem[1]);

						double newX = x * lonLen + lonStart;
						double newY = y * latLen + latStart;
						writer.write(newX + "," + newY + "\n");
					}

					// generate gap
					double lonGap = 30 + (60 - 30) * rand.nextDouble();
					double latGap = 30 + (60 - 30) * rand.nextDouble();
					lonStart = lonEnd + lonGap;
					latStart = latEnd + latGap;

					System.out.println("Write " + totalNumItem + " objects.");
				}
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void ShuffleFile(String inputPath, String outputPath) {
		String[] result = null;
		try (Stream<String> stream = Files.lines(Paths.get(inputPath))) {
			result = stream.toArray(String[]::new);
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<String> list = Arrays.asList(result);
//		System.out.println(result);
		Collections.shuffle(list);
//		System.out.println(result);
		
		try {
			FileWriter writer = new FileWriter(outputPath);
			for (String str: list) {
				writer.write(str + "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

	}

	public static void main(String[] args) {

		String folder = "/Users/xin_aurora/Downloads/Work/2019/"
				+ "UCR/Research/Spatial/sketches/vldb2024/expData/point/";
//		String filePath = folder + "uniform.csv";
//
//		int numOfLonPar = 5;
//		int numOfLatPar = 3;
//
//		int totalNumItem = 500000;
//		int totalNumItemMin = 5000;
//
//		GeneratedPartitionedData(numOfLonPar, numOfLatPar, folder, filePath, totalNumItem, totalNumItemMin);
		String inputFile = folder + "15clusters500K.csv";
		String outputFile = folder + "15clusters500K-shuffle.csv";
		ShuffleFile(inputFile, outputFile);
	}

}
