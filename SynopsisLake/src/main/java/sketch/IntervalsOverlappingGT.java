package sketch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import utils.UtilsFunction;
import utils.UtilsFunctionSketch;

public class IntervalsOverlappingGT {

	public IntervalsOverlappingGT(String ds1Path, String ds2Path) {
		ArrayList<Double> firstMin = new ArrayList<Double>();
		ArrayList<Double> firstMax = new ArrayList<Double>();

//		ArrayList<Double> secondMin = new ArrayList<Double>();
//		ArrayList<Double> secondMax = new ArrayList<Double>();

		double GT = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(ds1Path));
			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split(",");
				double minX = Double.parseDouble(tmp[0]);
				double minY = Double.parseDouble(tmp[1]);
				double maxX = Double.parseDouble(tmp[2]);
				double maxY = Double.parseDouble(tmp[3]);
				line = reader.readLine();
				firstMin.add(minX);
				firstMax.add(minY);
			}
			reader.close();

			reader = new BufferedReader(new FileReader(ds2Path));
			line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split(",");
				double minX = Double.parseDouble(tmp[0]);
				double minY = Double.parseDouble(tmp[1]);
				double maxX = Double.parseDouble(tmp[2]);
				double maxY = Double.parseDouble(tmp[3]);
				line = reader.readLine();
				for (int i = 0; i < firstMin.size(); i++) {
					if (UtilsFunction.isOverlapInterval(minX, maxX, firstMin.get(i), firstMax.get(i))) {
						GT++;
					}
				}

			}
			reader.close();
			System.out.println("GT = " + GT);
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
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/new_submission/data/";

		String ds1Path = folder + "bitSample.csv";
		String ds2Path = folder + "uniformSample.csv";
		
		IntervalsOverlappingGT run = new IntervalsOverlappingGT(ds1Path, ds2Path);
	}

}
