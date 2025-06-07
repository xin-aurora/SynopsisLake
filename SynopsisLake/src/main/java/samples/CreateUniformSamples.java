package samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateUniformSamples {

	public CreateUniformSamples(String inputFilePath, String outputFolder, int numberOfFile, int totalNumOfLine,
			double sampleRate) {
		int dataSizeBound = (int) Math.ceil(totalNumOfLine / (double)numberOfFile);
		File file = new File(inputFilePath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			ArrayList<String> dataList = new ArrayList<String>();
			int fileId = 0;
			String line = reader.readLine();
			while (line!= null) {
				dataList.add(line);
				line = reader.readLine();
				if (dataList.size() == dataSizeBound) {
					// create sample
					UniformSamples<String> sampler = new UniformSamples<String>(dataList.size(), sampleRate);
					List<String> samples = 	sampler.Sample(dataList);
					// write sample to file
					writeSampleData(outputFolder, fileId, samples);
					dataList = new ArrayList<String>();
					fileId++;
				}
			}
			
			if (dataList.size() > 0) {
				// create sample
				UniformSamples<String> sampler = new UniformSamples<String>(dataList.size(), 0.1);
				List<String> samples = sampler.Sample(dataList);
				// write sample to file
				writeSampleData(outputFolder, fileId, samples);
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
	
	private void writeSampleData(String outputFolder, int fileId, List<String> samples) {
		String filePath = outputFolder + fileId + ".csv";
		try {
			FileWriter writer = new FileWriter(filePath);
			for (String str : samples) {
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
				+ "UCR/Research/Spatial/sketches/vldb2024/expData/real-world/";
		String fileName = "wildfiredb_shuffle_sample.csv";
		String outputFolderName = "wildfiredb/";
		int numberOfFile = 10;
		int totalNumOfLine = 10000;
		double sampleRate = 0.01;

		for (int i = 0; i < args.length; i++) {
			if (args[i].equals("-folder")) {
				folder = args[++i];
			} else if (args[i].equals("-fileName")) {
				fileName = args[++i];
			} else if (args[i].equals("-outputFolderName")) {
				outputFolderName = args[++i];
			} else if (args[i].equals("-numberOfFile")) {
				numberOfFile = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-totalNumOfLine")) {
				totalNumOfLine = Integer.parseInt(args[++i]);
			} else if (args[i].equals("-sampleRate")) {
				sampleRate = Double.parseDouble(args[++i]);
			}
		}

		String inputFilePath = folder + fileName;
		String outputFolder = folder + outputFolderName;

		CreateUniformSamples run = new CreateUniformSamples(inputFilePath, outputFolder, numberOfFile, totalNumOfLine, sampleRate);
	}

}
