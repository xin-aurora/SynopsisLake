package loading.parquet;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ReadParquetFile {

	public static void main(String[] args) {

		
		String path = "/Users/xin_aurora/Downloads/Work/2019/UCR/Research/"
				+ "Spatial/sketches/code/FileFormat/FileFormat/test/ParuetFile/parquet";
		// TODO Auto-generated method stub
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		SparkSession spark = SparkSession.builder().
				appName("ParquetLoadingExample").config("spark.master", "local")
				.getOrCreate();

		// Read in the Parquet file created above.
		// Parquet files are self-describing so the schema is preserved
		// The result of loading a parquet file is also a DataFrame
		Dataset<Row> parquetFileDF = spark.read().parquet(path);

//		parquetFileDF.show();
		parquetFileDF.printSchema();
//		System.out.println(parquetFileDF.head());

//		parquetFileDF.createOrReplaceTempView("parquetFile");
//		Dataset<Row> resultDF = spark.sql("SELECT counts FROM parquetFile");
//
//		List<Row> rows = resultDF.collectAsList();
//		for (int i = 0; i < rows.size(); i++) {
////			System.out.println(rows.get(i));
//			Row r = rows.get(i);
//			
//		}

		spark.stop();
	}

}
