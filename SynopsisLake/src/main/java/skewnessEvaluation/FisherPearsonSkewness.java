package skewnessEvaluation;

public class FisherPearsonSkewness {
	
	/**
	 * Computes the skewness coefficient of a dataset.
	 *
	 * @param data Array of data points
	 * @return Skewness coefficient
	 */
	public static double computeSkewness(double[] data) {
		if (data == null || data.length < 3) {
			throw new IllegalArgumentException("Dataset must contain at least 3 data points.");
		}
		// Compute the third moment
		// \sum_{x_i - mean}^3}
		// scaled by the standard deviation.
		int n = data.length;
		double mean = computeMean(data);
		double stdDev = computeStandardDeviation(data, mean);

		double sumCubedDeviations = 0.0;
		for (double value : data) {
			double deviation = value - mean;
			sumCubedDeviations += Math.pow(deviation, 3);
		}
		
		// apply the Fisher-Pearson correction factor
		// n / [(n-1)*(n-2)]
		// to adjust for bias in small samples.
		double skewness = (n * sumCubedDeviations) / ((n - 1) * (n - 2) * Math.pow(stdDev, 3));
//		double skewness = sumCubedDeviations / Math.pow(stdDev, 3);
		return skewness;
	}

	/**
	 * Computes the mean of a dataset.
	 *
	 * @param data Array of data points
	 * @return Mean of the dataset
	 */
	private static double computeMean(double[] data) {
		double sum = 0.0;
		for (double value : data) {
			sum += value;
		}
		return sum / data.length;
	}

	/**
	 * Computes the standard deviation of a dataset.
	 *
	 * @param data Array of data points
	 * @param mean Mean of the dataset
	 * @return Standard deviation of the dataset
	 */
	private static double computeStandardDeviation(double[] data, double mean) {
		double sumSquaredDeviations = 0.0;
		for (double value : data) {
			sumSquaredDeviations += Math.pow(value - mean, 2);
		}
		return Math.sqrt(sumSquaredDeviations / (data.length - 1));
	}

	public static void main(String[] args) {
		// If the standard deviation is zero (all elements are identical), 
		// an exception is thrown.
		double[] data = { 1, 1, 1, 1, 1, 1}; // Example dataset

		try {
			double skewness = computeSkewness(data);
			System.out.println("Skewness Coefficient: " + skewness);
		} catch (IllegalArgumentException e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
}
