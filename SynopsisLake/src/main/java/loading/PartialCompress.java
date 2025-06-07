package loading;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import utils.ByteConvertLibrary;

public class PartialCompress {
	public static void main(String[] args) {
		String folder = "/Users/xin_aurora/Desktop/02_06/ch/";
		String file = folder + "file-0-1024.bin";
		String gzipFile = folder + "file-0-1024-partial.bin";
		String newFile = folder + "recovery-partial.bin";

		compressGzipFile(file, gzipFile);

//		decompressGzipFile(gzipFile, newFile);

	}

	private static void decompressGzipFile(String gzipFile, String newFile) {
		try {
			FileInputStream fis = new FileInputStream(gzipFile);
			GZIPInputStream gis = new GZIPInputStream(fis);
			FileOutputStream fos = new FileOutputStream(newFile);
			byte[] buffer = new byte[1024];
			int len;
			while ((len = gis.read(buffer)) != -1) {
				fos.write(buffer, 0, len);
			}
			// close resources
			fos.close();
			gis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void compressGzipFile(String filePath, String gzipFilePath) {
		try {
			File file = new File(filePath);
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			long fileLen = file.length();
			// Seek to the end of file
			raf.seek(fileLen - 4);
			// Read it out.
			byte[] synopsisLen = new byte[4];
			raf.read(synopsisLen, 0, 4);
			System.out.println("file length = " + fileLen);
			System.out.println(ByteConvertLibrary.ByteToInt(synopsisLen));
			int synLen = ByteConvertLibrary.ByteToInt(synopsisLen);
			byte[] synopsis = new byte[synLen];
			// Seek to the end of file
			raf.seek(fileLen - synLen);
			// Read it out.
			raf.read(synopsis, 0, synLen);
			System.out.println(synopsis.length);
			int startIdx = 33;
			int endIdx = startIdx + 5;
			byte[] synStartLen = Arrays.copyOfRange(synopsis, startIdx, endIdx);
			byte[] synoStart = Arrays.copyOfRange(synopsis, startIdx, endIdx - 1);
			System.out.println("synopsis start pos = " + ByteConvertLibrary.ByteToInt(synoStart));
			int recordLength = ByteConvertLibrary.ByteToInt(synoStart);
			FileInputStream fis = new FileInputStream(file);
			FileOutputStream fos = new FileOutputStream(gzipFilePath);
			GZIPOutputStream gzipOS = new GZIPOutputStream(fos);
			byte[] dataBuffer = new byte[recordLength];
			byte[] buffer = new byte[1024];
			fis.read(dataBuffer);
			fos.write(dataBuffer);	
			int len;
			while ((len = fis.read(buffer)) != -1) {
				gzipOS.write(buffer, 0, len);
			}
			// close resources
			gzipOS.close();
			fos.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
