package baseline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

public class Random1D {
	
	private double quality = -1;
	private int targetBoundarySize = -1;
	private double min = -1;
	private double max = -1;
	
	public Random1D(double quality, int targetSize, double min, double max) {
		this.quality = quality;
		this.targetBoundarySize = targetSize + 1;
		this.min = min;
		this.max = max;

	}
	
	public double[] reshapping(int randomRound) {

		double[] optArray = new double[targetBoundarySize];
		int round = 0;
		while (round < randomRound) {
			double[] array = randomPick();
			for (int i=0; i<array.length; i++) {
				optArray[i] += array[i];
			}
			round++;
		}
		
		for (int i=0; i<optArray.length; i++) {
			optArray[i] = optArray[i] / randomRound;
		}
		
//		System.out.println("Final array " + Arrays.toString(optArray));

		return optArray;
	}
	
	public double[] randomPick() {
		HashSet<Double> finalBoundaries = new HashSet<Double>(targetBoundarySize);
		finalBoundaries.add(min);
		finalBoundaries.add(max);
		
		double range = max - min;
		
		Random rand = new Random();
		
		while (finalBoundaries.size() < targetBoundarySize) {
			
			double val = rand.nextDouble() * range + min;

			finalBoundaries.add(val);

		}

		ArrayList<Double> boundaries = new ArrayList<Double>(finalBoundaries);
		Collections.sort(boundaries);

		double[] optArray = new double[boundaries.size()];
		for (int i = 0; i < optArray.length; i++) {
			optArray[i] = boundaries.get(i);
		}
		
//		System.out.println(Arrays.toString(optArray));
		return optArray;
	}


	public static void main(String[] args) {
		// TODO Auto-generated method stub
			
		int targetSize = 10;
		double min = 20.5;
		double max = 100.5;
		int randomRound = 10;

		Random1D rand = new Random1D(1, targetSize, min, max);
		
		rand.reshapping(randomRound);
	}

}
