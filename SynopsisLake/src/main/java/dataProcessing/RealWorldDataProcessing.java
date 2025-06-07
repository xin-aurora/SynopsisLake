package dataProcessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import org.apache.commons.text.diff.StringsComparator;

public class RealWorldDataProcessing {

	public static void ChicagoCrimes() {
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/data/real-world/";
		String inputPath = folder + "Chicago_Crimes.csv";
		String outputPath = folder + "Chicago_Crimes_Clean.csv";

		File file = new File(inputPath);
		FileWriter writer;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			writer = new FileWriter(new File(outputPath));
			String[] headerTmp = reader.readLine().split(",");
			String newHeader = headerTmp[0] + "\t" + headerTmp[1] + "\t" + headerTmp[4] + "\n";
			writer.write(newHeader);
			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split(",");
				if (tmp.length > 0) {
					String data = tmp[0] + "\t" + tmp[1] + "\t" + tmp[4] + "\n";
					writer.write(data);
				}
				line = reader.readLine();
			}
			writer.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void OSMPois() {
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/data/real-world/";
		String inputPath = folder + "osm21_pois.csv";
		String outputPath = folder + "osm21_pois_clean.csv";

		File file = new File(inputPath);
		FileWriter writer;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			writer = new FileWriter(new File(outputPath));
			String[] headerTmp = reader.readLine().split("\t");
			String newHeader = headerTmp[0] + "\t" + headerTmp[1] + "\t" + headerTmp[4] + "\n";
			writer.write(newHeader);
			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split("\t");
				if (tmp.length > 5) {
					String data = tmp[0] + "\t" + tmp[1] + "\t" + tmp[4] + "\n";
					writer.write(data);
				}
				line = reader.readLine();
			}
			writer.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void WidlFire() {
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/" + "Research/Spatial/sketches/data/real-world/";
		String inputPath = folder + "wildfiredb.csv";
		String outputPath = folder + "wildfiredb_clean.csv";

		File file = new File(inputPath);
		FileWriter writer;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			writer = new FileWriter(new File(outputPath));
			String[] headerTmp = reader.readLine().split("\t");
			String newHeader = headerTmp[0] + "\t" + headerTmp[1] + "\t" + headerTmp[3] + "\n";
			writer.write(newHeader);
			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split("\t");
				if (tmp.length > 4) {
					String data = tmp[0] + "\t" + tmp[1] + "\t" + tmp[3] + "\n";
					writer.write(data);
				}
				line = reader.readLine();
			}
			writer.close();
			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void RandomShuffle(String inputPath, String outputPath) {
		Path filePath = new File(inputPath).toPath();
		FileWriter writer;
		try {
			List<String> allLines = Files.readAllLines(filePath);
			writer = new FileWriter(new File(outputPath));
			writer.write(allLines.get(0) + "\n");
			int totalLines = allLines.size();
			allLines = allLines.subList(1, totalLines);
			Collections.shuffle(allLines);
			for (int i = 0; i < allLines.size(); i++) {
				writer.write(allLines.get(i) + "\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void timeRankWild(String inputPath, String outputPath) {

		File file = new File(inputPath);
		String newHeader = "";
		ArrayList<WildTimeHelper> list = new ArrayList<WildTimeHelper>();
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));

			String[] headerTmp = reader.readLine().split("\t");
			newHeader = headerTmp[0] + "\t" + headerTmp[1] + "\t" + headerTmp[2] + "\n";

			String line = reader.readLine();
			while (line != null) {
				String[] tmp = line.split("\t");
				if (tmp.length == 3) {
					String data = tmp[0] + "\t" + tmp[1] + "\t" + tmp[2] + "\n";
					list.add(new WildTimeHelper(data, tmp[2]));
				}

				line = reader.readLine();
			}

			reader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//		System.out.println(list);

		Collections.sort(list, new Comparator<WildTimeHelper>() {

			@Override
			public int compare(WildTimeHelper o1, WildTimeHelper o2) {
				// TODO Auto-generated method stub
				return Long.compare(o1.time, o2.time);
			}
		});

//		System.out.println(list);

		FileWriter writer;
		try {
			writer = new FileWriter(new File(outputPath));
			writer.write(newHeader);
			for (int i = 0; i < list.size(); i++) {
				writer.write(list.get(i).str);
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
//		ChicagoCrimes();
//		WidlFire();
//		OSMPois();

//		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/Spatial/sketches/data/real-world/sample/";
//		String inputPath = folder + "wildfiredb_sample.csv";
//		String outputPath = folder + "wildfiredb_clean_time.csv";

		String folder = args[0];
		String inputPath = folder + args[1];
		String outputPath = folder + args[2];
//		RandomShuffle(inputPath, outputPath);
		timeRankWild(inputPath, outputPath);
	}

}

class WildTimeHelper {
	public long time;
	public String str;

	public WildTimeHelper(String str, String timeStr) {
		this.str = str;
		this.time = Long.parseLong(timeStr.replaceAll("-", ""));
	}

	@Override
	public final String toString() {
		return str;
	}
}
