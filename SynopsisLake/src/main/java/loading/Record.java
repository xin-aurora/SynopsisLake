package loading;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Record implements Serializable {

	// each coordinate is a double, length 8 byte array
//	private byte[] coordinates; 
//	private byte[] arributes;

	private byte[] data;

	public Record(double coor1, double coor2, int attLen, byte[] attByte) {
		// put two coors
		// Double.BYTES * 2 + 4 = 16 + 4 = 20
		ByteBuffer byteBuffer = ByteBuffer.allocate(20 + attLen);
//		System.out.println("cap = " + byteBuffer.capacity());
		byteBuffer.putDouble(coor1);
		byteBuffer.putDouble(8, coor2);
		// put attribute length
		byteBuffer.putInt(16, attLen);
		// put attribute
		if (attLen > 0) {
			for (int i=0; i<attLen; i++) {
				byteBuffer.put(20 + i, attByte[i]);
			}
		}
		data = byteBuffer.array();
	}

	public Record(double coor1, double coor2, double coor3, double coor4, int attLen, byte[] attByte) {
		// put four coors
		// Double.BYTES * 4 + 4 = 32 + 4 = 36
		ByteBuffer byteBuffer = ByteBuffer.allocate(36 + attLen);
		byteBuffer.putDouble(coor1);
		byteBuffer.putDouble(8, coor2);
		byteBuffer.putDouble(16, coor3);
		byteBuffer.putDouble(24, coor4);
		// put attribute length
		byteBuffer.putInt(32, attLen);
		// put attribute
		if (attLen > 0) {
			for (int i=0; i<attLen; i++) {
				byteBuffer.put(36 + i, attByte[i]);
			}
		}
		data = byteBuffer.array();
	}
	
	public byte[] getData() {
		return data;
	}
	
	public int getLen() {
		return data.length;
	}

}
