package dataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class UpdateGT {

	public static void main(String[] args) {
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/"
				+ "Research/Spatial/sketches/exp/new_data/";
		String queryPath = folder + "synSmall-2p0-1024RQ.txt";
		String newQueryPath = folder + "synNew-2p0-1024RQ.txt";
		FileWriter myWriter;
		File file = new File(queryPath);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			myWriter = new FileWriter(newQueryPath, false);
			String str = reader.readLine();
			while (str != null) {
				String[] querytmp = str.split(",");
				double gt = Double.parseDouble(querytmp[4]) * 10;
				String newQuery = querytmp[0] + "," + querytmp[1] + ","
						+ querytmp[2] + "," + querytmp[3] + ","+ gt + "\n";
				myWriter.write(newQuery);
				str = reader.readLine();
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

}
