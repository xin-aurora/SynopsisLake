package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Figure {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/"
				+ "Work/2019/UCR/Research/Spatial/sketches/vldb2024/jar/";
		String filePath = folder + "15Cluster.txt";
		String outputPath = folder + "avg_variance.csv";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filePath));
			String str = "syn,";
			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split(",");
				System.out.println(tmp[tmp.length - 1].trim());
				str += Double.parseDouble(tmp[tmp.length - 1].trim());
				str += ",";
				line = reader.readLine();
			}
			reader.close();
			FileWriter myWriter = new FileWriter(outputPath, true);
			myWriter.write(str);
			myWriter.write("\n");
			myWriter.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
