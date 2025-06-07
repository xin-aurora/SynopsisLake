package sketch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class GetDataRange {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/new_submission/data/";

		String ds1Path = folder + "bit.csv";
		String ds2Path = folder + "uniform.csv";
		double minMinX = Double.MAX_VALUE;
		double maxMaxX = Double.MIN_VALUE;
		double minMinY = Double.MAX_VALUE;
		double maxMaxY = Double.MIN_VALUE;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(ds1Path));
			String line = reader.readLine();
			while (line !=null) {
				String[] tmp = line.split(",");
				double minX = Double.parseDouble(tmp[0]);
				double minY = Double.parseDouble(tmp[1]);
				double maxX = Double.parseDouble(tmp[2]);
				double maxY = Double.parseDouble(tmp[3]);
				minMinX = Math.min(minX, minMinX);
				minMinY = Math.min(minY, minMinY);
				maxMaxX = Math.max(maxX, maxMaxX);
				maxMaxY = Math.max(maxY, maxMaxY);
				line = reader.readLine();
			}
			reader.close();
			
			reader = new BufferedReader(new FileReader(ds2Path));
			line = reader.readLine();
			while (line !=null) {
				String[] tmp = line.split(",");
				double minX = Double.parseDouble(tmp[0]);
				double minY = Double.parseDouble(tmp[1]);
				double maxX = Double.parseDouble(tmp[2]);
				double maxY = Double.parseDouble(tmp[3]);
				minMinX = Math.min(minX, minMinX);
				minMinY = Math.min(minY, minMinY);
				maxMaxX = Math.max(maxX, maxMaxX);
				maxMaxY = Math.max(maxY, maxMaxY);
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
		
		System.out.println("minMinX=" + minMinX + ", maxMaxX=" + maxMaxX);
		System.out.println("minMinY=" + minMinY + ", maxMaxY=" + maxMaxY);
	}

}
