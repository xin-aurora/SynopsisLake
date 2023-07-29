package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class SyntheticDataGenerator {

	String fileFolder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/exp/ICDE_polygon/";

	String rawDataPath = "";

	String targetFolder = "";
	
	int dimension = 2;

	public SyntheticDataGenerator(int dimension, double[][] ranges, String rawDataName, String targetFolder) {
		this.dimension = dimension;
		System.out.println("d = " + this.dimension);
		this.rawDataPath = fileFolder + rawDataName;

		this.targetFolder = fileFolder + targetFolder;

		double[][] rawDataArray = readRawData(1000000);

		for (int i = 0; i < ranges.length; i++) {
			writeFile(i, ranges[i][0], ranges[i][1], rawDataArray);
		}
	}

	private void writeFile(int fileId, double min, double max, double[][] rawDataArray) {
		String filePath = targetFolder + fileId + ".csv";
		double range = max - min;
		try {
			FileWriter writer = new FileWriter(filePath);
			String header = min + "," + max + "\n";
			writer.write(header);
			for (int i = 0; i < rawDataArray.length; i++) {
				if (dimension == 2) {
					double x = rawDataArray[i][0];
					double y = rawDataArray[i][1];
					// pos = p * range + min
					double updatedX = x * range + min;
					double updatedY = y * range + min;
					String newData = updatedX + "," + updatedY + "\n";
					writer.write(newData);
				} else if (dimension == 4){
					double xMin = rawDataArray[i][0];
					double xMax = rawDataArray[i][1];
					double yMin = rawDataArray[i][2];
					double yMax = rawDataArray[i][3];
					// pos = p * range + min
					double updatedXMin = xMin * range + min;
					double updatedXMax = xMax * range + min;
					double updatedYMin = yMin * range + min;
					double updatedYMax = yMax * range + min;
					String newData = updatedXMin + "," 
					+ updatedXMax + ","
					+ updatedYMin + ","
					+ updatedYMax + "\n";
					writer.write(newData);
				}
				
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private double[][] readRawData(int dataSize) {
		double[][] dataArray = new double[dataSize][dimension];

		File file = new File(rawDataPath);

		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String line = reader.readLine();
			int idx = 0;
			while (line != null) {
				String[] tmp = line.split(",");
//				System.out.println(line);
				double[] pos = new double[dimension];
//				pos[0] = Double.parseDouble(tmp[0].trim());
//				pos[1] = Double.parseDouble(tmp[1].trim());
//				System.out.println(tmp.length);
				for (int i = 0; i < dimension; i++) {
					pos[i] = Double.parseDouble(tmp[i].trim());
				}
				dataArray[idx] = pos;
				idx++;
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

		return dataArray;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String fileFolder = "/Users/xin_aurora/Downloads/Work/"
				+ "2019/UCR/Research/Spatial/sketches/exp/ICDE_polygon/";

		String rawDataName = "bit03.csv";
		String targetFolder = "bit03/";

//		String rawDataName = "uniform.csv";
//		String targetFolder = "uniform/";

		double[][] ranges = { { 0.0, 20.20 }, { 15.0, 30.0 }, { 20.0, 35.0 }, { 30.0, 55.0 } };

		int dimension = 4;
		SyntheticDataGenerator run = new SyntheticDataGenerator(dimension, ranges, rawDataName, targetFolder);
	}

}
