package utils;

import java.util.ArrayList;
import java.util.Arrays;

import sketch.DyadicSpatialSketch;

public class UtilsFunctionSketch {

	/**
	 * select est Z * 4 = (X_II * Y_EE + X_IE * Y_EI + X_EI * Y_IE + X_EE * Y_II)
	 * X_II = sum_{Interval_X * Interval_Y} X_IE = sum_{Interval_X * Point_Y} X_EI =
	 * sum_{Point_X * Interval_Y} X_EE = sum_{Point_X * Point_Y}
	 * 
	 * @return
	 */
	public static double DyadicSpatialSketchSelectEst(DyadicSpatialSketch firstX, DyadicSpatialSketch firstY,
			DyadicSpatialSketch secondX, DyadicSpatialSketch secondY) {
		ArrayList<int[]> firstIntervalX = firstX.getIntervalsCoints();
		ArrayList<int[]> firstPointX = firstX.getPointCounters();
		ArrayList<int[]> firstIntervalY = firstY.getIntervalsCoints();
		ArrayList<int[]> firstPointY = firstY.getPointCounters();

		ArrayList<int[]> secondIntervalX = secondX.getIntervalsCoints();
		ArrayList<int[]> secondPointX = secondX.getPointCounters();
		ArrayList<int[]> secondIntervalY = secondY.getIntervalsCoints();
		ArrayList<int[]> secondPointY = secondY.getPointCounters();
		
		double XII = DyadicSpatialSketchSelectEstHelper(firstIntervalX, firstIntervalY);
		double XIE = DyadicSpatialSketchSelectEstHelper(firstIntervalX, firstPointY);
		double XEI = DyadicSpatialSketchSelectEstHelper(firstPointX, secondIntervalY);
		double XEE = DyadicSpatialSketchSelectEstHelper(firstPointY, firstPointY);

		double YII = DyadicSpatialSketchSelectEstHelper(secondIntervalX, secondIntervalY);
		double YIE = DyadicSpatialSketchSelectEstHelper(secondIntervalX, secondPointY);
		double YEI = DyadicSpatialSketchSelectEstHelper(secondPointX, secondIntervalY);
		double YEE = DyadicSpatialSketchSelectEstHelper(secondPointX, secondPointY);

		double result = (XII * YEE + XIE * YEI + XEI * YIE + XEE * YII) / 4;

		return result;
	}
	
	public static double DyadicSpatialSketchSelectEst(DyadicSpatialSketch firstX, 
			DyadicSpatialSketch secondX) {
		ArrayList<int[]> firstIntervalX = firstX.getIntervalsCoints();
		ArrayList<int[]> firstPointX = firstX.getPointCounters();
		ArrayList<int[]> secondIntervalX = secondX.getIntervalsCoints();
		ArrayList<int[]> secondPointX = secondX.getPointCounters();
		
		double XI = DyadicSpatialSketchSelectEstHelper(firstIntervalX, secondPointX);
		double XE = DyadicSpatialSketchSelectEstHelper(firstPointX, secondIntervalX);

		double result = (XI + XE) / 2;

		return result;
	}

	public static double DyadicSpatialSketchSelectEstHelper(ArrayList<int[]> first, ArrayList<int[]> second) {
		double result = 0.0;
		for (int level = 0; level < first.size(); level++) {
			int[] firstCounters = first.get(level);
			int[] secondCounters = second.get(level);
			for (int i = 0; i < firstCounters.length; i++) {
				result += firstCounters[i] * secondCounters[i];
			}
		}
		
//		System.out.println("result = " + result);
		return result;
	}
}
