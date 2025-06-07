package loading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;

import utils.ByteConvertLibrary;

public class ReadFile {

	public ReadFile(String filePath1, String filePath2) {

		File fileNoSyno = new File(filePath1);

		File file = new File(filePath2);
		try {

			RandomAccessFile raf = new RandomAccessFile(file, "r");
			long fileLen = file.length();
			// Seek to the end of file
			raf.seek(fileLen - 4);
			// Read it out.
			byte[] synopsisLen = new byte[4];
			raf.read(synopsisLen, 0, 4);

			System.out.println(ByteConvertLibrary.ByteToInt(synopsisLen));

			int synLen = ByteConvertLibrary.ByteToInt(synopsisLen);
			byte[] synopsis = new byte[synLen];
			// Seek to the end of file
			raf.seek(fileLen - synLen);
			// Read it out.
			raf.read(synopsis, 0, synLen);

			System.out.println(synopsis.length);

//			int tailSize = 1 + 32 + firstSynosisLines.length * 5 + 4;
//			byteBuffer = ByteBuffer.allocate(tailSize);
//			byteBuffer.put(recordDimension);
//			byteBuffer.putDouble(1, MinMinLon);
//			byteBuffer.putDouble(9, MinMinLat);
//			byteBuffer.putDouble(17, MaxMaxLon);
//			byteBuffer.putDouble(25, MaxMaxLat);
//			int curIdx = 33;
//			for (int i = 0; i < firstSynosisLines.length; i++) {
//				byteBuffer.putInt(curIdx, firstSynosisLines[i]);
//				curIdx += 4;
//				byteBuffer.put(curIdx, synopsisType[i]);
//				curIdx += 1;
//			}
//			byteBuffer.putInt(curIdx, tailSize);
			byte recordDimension = synopsis[0];
			int dimension = recordDimension;
			System.out.println("recordDimension = " + dimension);
			byte[] minLon = Arrays.copyOfRange(synopsis, 1, 8);
			byte[] minLat = Arrays.copyOfRange(synopsis, 9, 16);
			byte[] maxLon = Arrays.copyOfRange(synopsis, 17, 24);
			byte[] maxLat = Arrays.copyOfRange(synopsis, 25, 32);

			int numSyno = (synLen - 33 - 4) / 5;
			System.out.println("numSyno = " + numSyno);
			for (int sId = 0; sId < numSyno; sId++) {
				int startIdx = 33 + sId * 5;
				int endIdx = startIdx + 5;
				byte[] synStartLen = Arrays.copyOfRange(synopsis, startIdx, endIdx);
				byte synType = synStartLen[4];
				byte[] synoStart = Arrays.copyOfRange(synopsis, startIdx, endIdx - 1);
				System.out.println("synopsis start pos = " + ByteConvertLibrary.ByteToInt(synoStart));
				System.out.println("synopsis type = " + Byte.toUnsignedInt(synType));
			}

			// overhead
			long fileNoSynoLen = fileNoSyno.length();
			double overhead = (fileLen - fileNoSynoLen) / (double) fileNoSynoLen;
			System.out.println("overhead = " + overhead);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ReadFile(String filePath) {
		File file = new File(filePath);
		try {

			RandomAccessFile raf = new RandomAccessFile(file, "r");
			long fileLen = file.length();
			// Seek to the end of file
			raf.seek(fileLen - 4);
			// Read it out.
			byte[] synopsisLen = new byte[4];
			raf.read(synopsisLen, 0, 4);

			System.out.println("footer length = " + ByteConvertLibrary.ByteToInt(synopsisLen));

			System.out.println("file length = " + fileLen);


		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String folder = "/Users/xin_aurora/Desktop/02_06/ch/";
//		String filePath2 = folder + "file-0-wave.bin";
//		String filePath2 = folder + "file-0-NoFre.bin";
		String filePath2 = folder + "file-0-1024.bin";
//		String filePath2 = folder + "file-0-128.bin";
		String filePath1 = folder + "file-0-Nosyno.bin";
		ReadFile read = new ReadFile(filePath2, filePath2);
	}

}
