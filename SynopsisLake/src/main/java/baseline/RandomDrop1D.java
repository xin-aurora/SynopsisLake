package baseline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;

public class RandomDrop1D {

	private double quality = -1;
	private int targetBoundarySize = -1;
	private double min = -1;
	private double max = -1;

	public RandomDrop1D(double quality, int targetSize, double min, double max) {
		this.quality = quality;
		this.targetBoundarySize = targetSize + 1;
		this.min = min;
		this.max = max;

	}

	public double[] reshappingFromArray(ArrayList<double[]> sources) {

		ArrayList<Double> initialBoundaries = new ArrayList<Double>();

		for (int i = 0; i < sources.size(); i++) {
			double[] boundaries = sources.get(i);
			for (double b : boundaries) {
				initialBoundaries.add(b);
			}
		}

		HashSet<Double> finalBoundaries = new HashSet<Double>(targetBoundarySize);
		finalBoundaries.add(min);
		finalBoundaries.add(max);
		int initialBoundariesSize = initialBoundaries.size();
		Random rand = new Random();
		int totalNumB = targetBoundarySize;
		while (finalBoundaries.size() < totalNumB) {
			int idx = rand.nextInt(initialBoundariesSize);
			double val = initialBoundaries.get(idx);

			finalBoundaries.add(val);

		}

		ArrayList<Double> boundaries = new ArrayList<Double>(finalBoundaries);
		Collections.sort(boundaries);

		double[] optArray = new double[boundaries.size()];
		for (int i = 0; i < optArray.length; i++) {
			optArray[i] = boundaries.get(i);
		}

		return optArray;
	}

	public double[] reshapping(ArrayList<ArrayList<Double>> sources) {

		ArrayList<Double> initialBoundaries = new ArrayList<Double>();

		for (int i = 0; i < sources.size(); i++) {
			initialBoundaries.addAll(sources.get(i));
		}

		HashSet<Double> finalBoundaries = new HashSet<Double>(targetBoundarySize);
		finalBoundaries.add(min);
		finalBoundaries.add(max);
		int initialBoundariesSize = initialBoundaries.size();
		Random rand = new Random();
		int totalNumB = targetBoundarySize;
		while (finalBoundaries.size() < totalNumB) {
			int idx = rand.nextInt(initialBoundariesSize);
			double val = initialBoundaries.get(idx);

			finalBoundaries.add(val);

		}

		ArrayList<Double> boundaries = new ArrayList<Double>(finalBoundaries);
		Collections.sort(boundaries);

		double[] optArray = new double[boundaries.size()];
		for (int i = 0; i < optArray.length; i++) {
			optArray[i] = boundaries.get(i);
		}
		return optArray;

	}

	public ArrayList<Double> reshappingList(ArrayList<ArrayList<Double>> sources) {

		ArrayList<Double> initialBoundaries = new ArrayList<Double>();

		for (int i = 0; i < sources.size(); i++) {
			initialBoundaries.addAll(sources.get(i));
		}

		HashSet<Double> finalBoundaries = new HashSet<Double>(targetBoundarySize);
		int initialBoundariesSize = initialBoundaries.size();
		Random rand = new Random();
		finalBoundaries.add(min);
		finalBoundaries.add(max);
		int totalNumB = targetBoundarySize;
		while (finalBoundaries.size() < totalNumB) {
			int idx = rand.nextInt(initialBoundariesSize);
			double val = initialBoundaries.get(idx);
			if ((val != min) && (val != max)) {
				finalBoundaries.add(val);
			}
		}

		ArrayList<Double> boundaries = new ArrayList<Double>(finalBoundaries);
		Collections.sort(boundaries);

		return boundaries;

	}

}