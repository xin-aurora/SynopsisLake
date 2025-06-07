package dataProcessing;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class RangeQueryMake {

	public static void pickVarying() {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/" + "2019/UCR/Research/Spatial/sketches/vldb2024/"
				+ "expResult/query/strick/";
		String keep = folder + "ebird-20-1024RQCountKGood.txt";
		String filter = folder + "ebird-20-1024RQCountKBad.txt";
		String outputPath = folder + "ebird-20-1024RQCount-2.txt";
		ArrayList<String> keepList = new ArrayList<String>();
		ArrayList<String> filterList = new ArrayList<String>();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(keep));
			String query = reader.readLine();
			while (query != null) {
				keepList.add(query);
				query = reader.readLine();
			}
			reader.close();
			reader = new BufferedReader(new FileReader(filter));
			query = reader.readLine();
			while (query != null) {
				filterList.add(query);
				query = reader.readLine();
			}
			reader.close();
			int keepLen = keepList.size();
			int filterLen = filterList.size();
			System.out.println("keep List size = " + keepLen);
			System.out.println("filter List size = " + filterLen);
			FileWriter writer = new FileWriter(outputPath);
			int total = keepLen + filterLen;
			Random rand = new Random();
			int pickFilter = 0;
			int pickKeep = 0;
			int cnt = 0;
			while (cnt < filterLen) {
				int idx = rand.nextInt(total);
				if (idx < keepLen) {
					writer.write(keepList.get(idx) + "\n");
					pickKeep++;
				} else {
					idx = idx - keepLen;
					writer.write(filterList.get(idx) + "\n");
					pickFilter++;
				}
				cnt++;
			}
			for (String str : keepList) {
				writer.write(str + "\n");
			}
			writer.close();
			System.out.println("pickFilter = " + pickFilter + ", pickKeep = " + pickKeep);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void pickOnly() {
		// TODO Auto-generated method stub
		String folder = "/Users/xin_aurora/Downloads/Work/" + "2019/UCR/Research/Spatial/sketches/vldb2024/"
				+ "expResult/query/strick/";
		String keep = folder + "ebird-20-1024RQCountKGood.txt";
		String filter = folder + "ebird-20-1024RQCountKBad.txt";
		String outputPath = folder + "ebird-20-1024RQCount.txt";
		ArrayList<String> keepList = new ArrayList<String>();
		ArrayList<String> filterList = new ArrayList<String>();

		try {
			BufferedReader reader = new BufferedReader(new FileReader(keep));
			String query = reader.readLine();
			while (query != null) {
				keepList.add(query);
				query = reader.readLine();
			}
			reader.close();
			reader = new BufferedReader(new FileReader(filter));
			query = reader.readLine();
			while (query != null) {
				filterList.add(query);
				query = reader.readLine();
			}
			reader.close();
			int keepLen = keepList.size();
			int filterLen = filterList.size();
			System.out.println("keep List size = " + keepLen);
			System.out.println("filter List size = " + filterLen);
			FileWriter writer = new FileWriter(outputPath);
			
			Random rand = new Random();
			int pickFilter = 0;
			int pickKeep = 0;
			int cnt = 0;
			while (cnt < filterLen) {
				int idx = rand.nextInt(keepLen);
				writer.write(keepList.get(idx) + "\n");
				pickKeep++;
				cnt++;
			}
			for (String str : keepList) {
				writer.write(str + "\n");
			}
			writer.close();
			System.out.println("pickFilter = " + pickFilter + ", pickKeep = " + pickKeep);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		pickOnly();
		pickVarying();
	}

}
