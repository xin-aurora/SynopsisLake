package dataProcessing;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class eBirdDataAnalysisClean {

	public eBirdDataAnalysisClean(String filePath, String outputPath,
			double sampleRate) {
		getPartOfDS(filePath, outputPath, sampleRate);
	}
	
	private void getPartOfDS(String filePath, String outputPath
			, double sampleRate) {
		Random rand = new Random();
		ArrayList<String> sampledData = new ArrayList<String>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();
			
			line = reader.readLine();
			while (line != null) {
				double s = rand.nextDouble();
				if (s < sampleRate) {
					sampledData.add(line);
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
		System.out.println("number of items = " + sampledData.size());
		Collections.shuffle(sampledData);
		try {
			FileWriter writer = new FileWriter(outputPath);
			for (String str : sampledData) {
				writer.write(str + "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void createFilteredDS(String filePath, String outputPath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			FileWriter writer = new FileWriter(outputPath);
			String line = reader.readLine();
			String[] header = line.split(",");
			String newHeader = header[0] + "," + header[1] + "," + header[4] + "," + header[10] + "," + header[14] + ","
					+ header[15] + "," + header[16] + "," + header[18] + "\n";
			writer.write(newHeader);
			// 0: x, 1: y, 4: TAXONOMIC ORDER, 10: OBSERVATION COUNT
			// 14: COUNTRY, 15: COUNTRY CODE
			// 16: STATE, 17: STATE CODE, 18: COUNTY
			line = reader.readLine();
			int cnt = 0;
			while (line != null) {
				String[] tmp = line.split(",");
				if (tmp[0].length() > 0 && tmp[1].length() > 0) {
					if (tmp[10].equals("X")) {
						tmp[10] = "0";
					}
					String newLine = tmp[0] + "," + tmp[1] + "," + tmp[4] + "," +
							tmp[10] + "," + tmp[14] + "," + tmp[16] + ","
							+ tmp[17] + "," + tmp[18] + "\n";
					writer.write(newLine);
				}
				line = reader.readLine();
				cnt++;
				if (cnt % 50000000 == 0) {
					System.out.println("process " + cnt + " items.");
					writer.close();
					writer = new FileWriter(outputPath, true);
				}
			}
			writer.close();
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void checkDS(String filePath) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String line = reader.readLine();
			String[] header = line.split(",");
			System.out.println("number of attribute = " + header.length);
			for (int i = 0; i < header.length; i++) {
				System.out.println(i + ": " + header[i]);
			}
			// 0: x, 1: y, 4: TAXONOMIC ORDER, 10: OBSERVATION COUNT
			// 14: COUNTRY, 15: COUNTRY CODE
			// 16: STATE, 17: STATE CODE, 18: COUNTY

			for (int i = 0; i < 10; i++) {
				line = reader.readLine();
				String[] tmp = line.split(",");
				System.out.println(header[0] + ": " + tmp[0] + ", " + header[1] + ": " + tmp[1] + ", " + header[4]
						+ ": " + tmp[4] + ", " + header[10] + ": " + tmp[10] + ", " + header[14] + ": " + tmp[14] + ", "
						+ header[16] + ": " + tmp[16] + ", " + header[17] + ": " + tmp[17] + ", " + header[18] + ": "
						+ tmp[18] + ", ");
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/home/xzhan261/vldb2024/";
		String filePath = "ebirdFilter.csv";
		String outputPath = "ebird10.csv";
//		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/vldb2024/jar/";
//		String filePath = "ebirdSample.csv";
//		String outputPath = "ebirdSampleClean.csv";

		filePath = folder + filePath;
		outputPath = folder + outputPath;
		double sampleRate = 0.1;
		eBirdDataAnalysisClean run = new eBirdDataAnalysisClean(filePath, outputPath, sampleRate);
	}

}
