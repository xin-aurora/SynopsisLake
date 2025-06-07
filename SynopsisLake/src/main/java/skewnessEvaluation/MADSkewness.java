package skewnessEvaluation;

import java.util.Arrays;

public class MADSkewness {
	
	public static double MADSkewnessTestUniform(double[] data) {
		double median = CalculateMedian(data);

		// Calculate absolute deviations from the median
		double[] deviations = new double[data.length];
		for (int i = 0; i < data.length; i++) {
			deviations[i] = Math.abs(data[i] - median);
		}
		double MADS = CalculateMedian(deviations);
		return MADS / median;
	}
	
	public static double CalculateMedian(double[] values) {
		double[] dummyArray = Arrays.copyOf(values, values.length);
		Arrays.sort(dummyArray);
		int n = dummyArray.length;
		if (n % 2 == 0) {
			return (dummyArray[n / 2 - 1] + dummyArray[n / 2]) / 2.0;
		} else {
			return dummyArray[n / 2];
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Example data
        double[] sampleData = {2.1, 2.4, 3.0, 5.2, 5.3, 5.9, 7.8, 9.9, 10.0, 10.1};
        // Compute MADSkewness statistic
        double MADSkewnessStat = MADSkewnessTestUniform(sampleData);
        System.out.println("MAD Skewness statistic (Uniform) = " + MADSkewnessStat);
	}

}
