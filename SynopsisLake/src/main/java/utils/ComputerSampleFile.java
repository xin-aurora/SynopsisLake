package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class ComputerSampleFile {
	
	public ComputerSampleFile(String filePath, double sampleRate, String outputPath) {
//		int totalSize = 147331045;
		int totalSize = 80074787;
		
		int totalWriteSize = (int) (totalSize * sampleRate);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			FileWriter writer = new FileWriter(outputPath);
			// remove header
			reader.readLine();
			String line = reader.readLine();
			int cnt = 0;
			while (cnt < totalWriteSize) {
//				String[] tmp = line.split("\t");
//				String newStr = tmp[0].trim() + "," + tmp[1].trim() + "\n";
//				writer.write(newStr);
				writer.write(line);
				line = reader.readLine();
				cnt++;
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/home/xzhan261/synopsis/data/";
		String filePath = folder + "osm21_pois_clean_shuffle.csv"; 
		double sampleRate = 0.1;
		String outputPath = "";
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-sampleRate")) {
				sampleRate = Double.parseDouble(args[++i]);
			}  else if (args[i].equals("-outputPath")) {
				outputPath = args[++i];
			} 
		}
		
		ComputerSampleFile run = new ComputerSampleFile(filePath, sampleRate, outputPath);
	}

}
