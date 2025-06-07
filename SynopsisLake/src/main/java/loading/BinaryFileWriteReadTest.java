package loading;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class BinaryFileWriteReadTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkConf sparkCond = new SparkConf().setAppName("fileTest");
		sparkCond.set("spark.driver.memory", "100g");
		sparkCond.set("spark.memory.offHeap.enabled", "true");
		sparkCond.set("spark.memory.offHeap.size", "16g");
		sparkCond.set("spark.rpc.message.maxSize", "1024");
		// Set Spark master to local if not already set
		if (!sparkCond.contains("spark.master"))
			sparkCond.setMaster("local[4]");
		JavaSparkContext sc = new JavaSparkContext(sparkCond);

		SparkSession spark = SparkSession.builder().config(sc.getConf()).getOrCreate();
		
		String folder = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/vldb2024/expData/real-world/";
		String inputPath = folder + "osm21_pois_shuffle_sample.csv";
		String outputPath = folder + "sampleBin.bin";
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputPath));
			FileOutputStream fos = new FileOutputStream(outputPath);
			String line = reader.readLine();
			while (line != null) {
				line += "\n";
				// write data
				String[] tmp = line.split("\t");
				double x = Double.parseDouble(tmp[0]);
				double y = Double.parseDouble(tmp[1]);
				ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES * 2);
		        buffer.putDouble(x);
		        buffer.putDouble(y);
				fos.write(buffer.array());
				line = reader.readLine();
			}
			reader.close();
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		File inputFile = new File(outputPath);
		try {
			FileInputStream fis = new FileInputStream(inputFile);
			byte[] bufferRead = new byte[Double.BYTES*2];
			int bytesRead;
			while ((bytesRead = fis.read(bufferRead)) != -1) {
				ByteBuffer buffer = ByteBuffer.wrap(bufferRead);
				double x = buffer.getDouble();
				double y = buffer.getDouble();
				System.out.println(x + ", " + y);
            }
			fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		// read by spark
//		JavaPairRDD<String, PortableDataStream> binaryFileRDD = sc.binaryFiles(outputPath);
//		JavaRDD<String[]> content = binaryFileRDD.map(new Function<Tuple2<String,PortableDataStream>, String[]>() {
//
//			@Override
//			public String[] call(Tuple2<String, PortableDataStream> pair) throws Exception {
//				// TODO Auto-generated method stub
//				String path = pair._1;
//				byte[] content = pair._2.toArray();
//				String recover = new String(content);
//				String[] tmp = recover.split("\n");
//				return tmp;
//			}
//		});
//		System.out.println(content.collect().get(0)[0]);
//		System.out.println(content.collect().get(0).length);
		sc.close();
	}

}
