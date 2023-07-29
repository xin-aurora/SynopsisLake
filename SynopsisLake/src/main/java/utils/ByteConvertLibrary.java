package utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class ByteConvertLibrary {

	public static byte[] DoubleToByte(double d) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
		byteBuffer.putDouble(d);
		return byteBuffer.array();
	}
	
	public static byte[] DoubleToByte(double d1, double d2) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES*2);
		byteBuffer.putDouble(d1);
		byteBuffer.putDouble(8, d2);
		return byteBuffer.array();
	}

	public static double ByteToDouble(byte[] byteArray) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Double.BYTES);
		byteBuffer.put(byteArray);
		byteBuffer.flip();
		return byteBuffer.getDouble();
	}

	public static byte[] IntToByte(int i) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
		byteBuffer.putInt(i);
		return byteBuffer.array();
	}

	public static int ByteToInt(byte[] byteArray) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
		byteBuffer.put(byteArray);
		byteBuffer.flip();
		return byteBuffer.getInt();
	}
	
	public static long ByteTolong(byte[] byteArray) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
		byteBuffer.put(byteArray);
		byteBuffer.flip();
		return byteBuffer.getLong();
	}

	public static byte[] StringToByte(String s) {
		return s.getBytes(StandardCharsets.UTF_8);
	}

	public static String ByteToString(byte[] byteArray) {
		return new String(byteArray, StandardCharsets.UTF_8);
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double d = 65.43;

		byte[] dArray = ByteConvertLibrary.DoubleToByte(d);
		System.out.println(dArray.length);
		
		System.out.println(ByteConvertLibrary.DoubleToByte(0.6, 0.4).length);

		int i = 3456;
		byte[] iArray = ByteConvertLibrary.IntToByte(i);
		System.out.println(iArray.length);

		String s1 = "abcdefg";
		byte[] s1Array = ByteConvertLibrary.StringToByte(s1);
		System.out.println(s1Array.length);

		System.out.println(ByteConvertLibrary.ByteToString(s1Array));

		String s2 = "abc";
		byte[] s2Array = ByteConvertLibrary.StringToByte(s2);
		System.out.println(s2Array.length);

		System.out.println(ByteConvertLibrary.ByteToString(s2Array));
		
		String s0 = "";
		byte[] s0Array = ByteConvertLibrary.StringToByte(s0);
		System.out.println(s0Array.length);
	}

}
