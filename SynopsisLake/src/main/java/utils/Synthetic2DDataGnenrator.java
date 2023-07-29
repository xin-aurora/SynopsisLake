package utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class Synthetic2DDataGnenrator {

	double minLon = -110;
	double minLat = 35;
	double maxLon = -65;
	double maxLat = 45;

	double lonRange = 45;
	double latRange = 10;

	public Synthetic2DDataGnenrator(String folder, String rawDataName, String targetFolder, int newFileId,
			double minLonNew, double minLatNew, double maxLonNew, double maxLatNew,
			int newSize) {
		double lonRangeNew = maxLonNew - minLonNew;
		double latRangeNew = maxLatNew - minLatNew;
		loadData(folder, rawDataName, targetFolder, newFileId, 
				lonRangeNew, latRangeNew, 
				minLonNew, minLatNew, newSize, 
				maxLonNew, maxLatNew);
	}

	private void loadData(String folder, String rawDataName, 
			String targetFolder, int newFileId,
			double lonRangeNew, double latRangeNew,
			double minLonNew, double minLatNew, int newSize,
			double maxLonNew, double maxLatNew) {
		String filePath = folder + rawDataName;

		String newFile = folder + targetFolder + "/" + newFileId + ".csv";
		
		String newFileAll = folder + targetFolder + "/" + targetFolder + ".csv";
		
		ArrayList<double[]> data = new ArrayList<double[]>();

		File file = new File(filePath);
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(file));

			// header
			String line = reader.readLine();
			line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split("\t");
				double lon = Double.parseDouble(tmp[0]);
				double lat = Double.parseDouble(tmp[1]);
				
				double[] pos = new double[2];
				pos[0] = lon;
				pos[1] = lat;
				data.add(pos);
				
				line = reader.readLine();
			}
			reader.close();
			
			
			Collections.shuffle(data);
			
			FileWriter writer = new FileWriter(newFile);
			String header = minLonNew + "," + maxLonNew + "," + minLatNew + ","
					+ maxLatNew + "\n";
			writer.write(header);
			
			FileWriter writerAll = new FileWriter(newFileAll, true);
			
			for (int i=0; i<newSize; i++) {
				
				double[] pos = data.get(i);
				double lon = pos[0];
				double lat = pos[1];
				
				// new pos
				double newLon = ( lon - minLon ) / lonRange * lonRangeNew + minLonNew;
				double newLat = ( lat - minLat ) / latRange * latRangeNew + minLatNew;
				
				String newLine = newLon + "\t" + newLat + "\n";
				
				writer.write(newLine);
				writerAll.write(newLine);
			}
			
			writer.close();
			writerAll.close();

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

		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/exp/ICDE/";
		String rawDataName = "wildfiredb_sample.txt";
		String targetFolder = "wildfire";

		double minLonNew = -80;
		double minLatNew = 42;
		double maxLonNew = -35;
		double maxLatNew = 55;
		
		int fileId = 0;
		
		int newSize = 100;
		
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-rawDataName")) {
				rawDataName = args[++i];
			} else if (args[i].equals("-targetFolder")) {
				targetFolder = args[++i];
			} else if (args[i].equals("-minLon")) {
				minLonNew = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-minLat")) {
				minLatNew = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-maxLon")) {
				maxLonNew = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-maxLat")) {
				maxLatNew = Double.parseDouble(args[++i]);
			} else if (args[i].equals("-fileId")) {
				fileId = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-newSize")) {
				newSize = Integer.parseInt(args[++i]);
			}
		}

		Synthetic2DDataGnenrator run = new Synthetic2DDataGnenrator(folder, rawDataName, targetFolder, fileId, 
				minLonNew, minLatNew, maxLonNew, maxLatNew, newSize);
				
	}

}
