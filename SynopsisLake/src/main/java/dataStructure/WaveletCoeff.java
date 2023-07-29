package dataStructure;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class WaveletCoeff implements Serializable{

	private double val;
	private int key;
	// total height = logM, m: len of data array
	// resolution level = total height - 1 - height
	private int[] height;

	public WaveletCoeff(double val, int[] height, int key) {
		this.val = val;
		this.key = key;
		this.height = height;
	}

	public double getVal() {
		return this.val;
	}

	public int[] getHeight() {
		return this.height;
	}

	public int getKey() {
		return this.key;
	}

	public void updateVal(double normVal) {
		this.val = normVal;
	}

	@Override
	public final String toString() {
		return String.valueOf(val) + "-" + Arrays.toString(height) + "-" + key + ";";
	}

	public byte[] getByteArray() {
		// val + key + height.length + height
		int length = 8 + 4 + 4 + height.length * 4;
		ByteBuffer buffer = ByteBuffer.allocate(length);
		buffer.putDouble(val);
		buffer.putInt(8, key);
		buffer.putInt(12, height.length);
		for (int i = 16; i < height.length; i++) {
			buffer.putInt(16 + i * 4, height[i]);
		}
		return buffer.array();
	}

}
