package dataProcessing;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import dataStructure.File.LoadFile;

public class LoadDataFiles {
	
	public static void LoadSynethicDataset() {
		
	}
	
	public static void LoadRealWorldDataset() {
		int dataSizeBound = 1000;
		boolean haveSynopsis = true;

		int[] synopsisType = {0};
		int[][] synopsisSize = {{ 5, 5 }};

		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/data/real-world/";
		String inputPath = folder + "Chicago_Crimes_Clean.csv";
		String outputPath = folder + "Chicago_Crimes_file.csv";

		Path filePath = new File(inputPath).toPath();

		try {
			List<String> allLines = Files.readAllLines(filePath);
			List<String> firstList = allLines.subList(1, dataSizeBound + 1);

			LoadFile load = new LoadFile(dataSizeBound, haveSynopsis, ",");
			load.setSynopsis(synopsisType, synopsisSize);
			load.buildFile(outputPath, firstList);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		

	}

}
