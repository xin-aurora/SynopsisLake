package utils;

import java.util.Arrays;
import java.util.Comparator;

public class SpearmanCorrelation {
	/**
	 * Computes the Spearman rank correlation coefficient between two arrays.
	 * 
	 * @param x the first array of values
	 * @param y the second array of values
	 * @return Spearman's rank correlation coefficient
	 * @throws IllegalArgumentException if the arrays have different lengths or are
	 *                                  empty
	 */
	public static double spearmanCorrelation(double[] x, double[] y) {
		if (x == null || y == null || x.length == 0 || y.length == 0) {
			throw new IllegalArgumentException("Input arrays cannot be null or empty");
		}
		if (x.length != y.length) {
			throw new IllegalArgumentException("Input arrays must have the same length");
		}

		// 1. Get the ranks for both arrays
		double[] rx = getRanks(x);
		double[] ry = getRanks(y);

		// 2. Compute the sum of squared differences of ranks
		int n = x.length;
		double sumD2 = 0.0;
		for (int i = 0; i < n; i++) {
			double d = rx[i] - ry[i];
			sumD2 += d * d;
		}

		// 3. Apply the Spearman formula
		// rho = 1 - (6 * sumD2) / [n(n^2 - 1)]
		double numerator = 6.0 * sumD2;
		double denominator = n * (n * n - 1.0);

		return 1.0 - (numerator / denominator);
	}

	/**
	 * Returns an array of ranks for the given array of values. Ties are handled by
	 * assigning the average rank to tied values.
	 *
	 * Example: for values [10, 20, 20, 40], the sorted order is [10, 20, 20, 40].
	 * The ranks (1-based) would be: 1 for 10, and the two 20s share ranks 2 and 3,
	 * so each 20 gets average rank = (2+3)/2 = 2.5, and 40 gets rank 4.
	 *
	 * @param values array of values
	 * @return array of ranks corresponding to the input values
	 */
	private static double[] getRanks(double[] values) {
		// Prepare an array of indices
		Integer[] indices = new Integer[values.length];
		for (int i = 0; i < values.length; i++) {
			indices[i] = i;
		}

		// Sort indices according to values
		Arrays.sort(indices, Comparator.comparingDouble(i -> values[i]));

		// Create an array to store the ranks
		double[] ranks = new double[values.length];

		// Assign ranks; handle ties by assigning the average rank
		int i = 0;
		while (i < values.length) {
			int startIndex = i;
			double currentValue = values[indices[i]];

			// Find how many have the same value (tie)
			int endIndex = i;
			while (endIndex < values.length && values[indices[endIndex]] == currentValue) {
				endIndex++;
			}

			// All items from i to endIndex-1 have the same value
			double avgRank = (startIndex + 1 + endIndex) / 2.0; // 1-based rank average

			// Assign the average rank to all tied values
			for (int j = startIndex; j < endIndex; j++) {
				ranks[indices[j]] = avgRank;
			}

			i = endIndex;
		}
		return ranks;
	}

	// --- Test the code ---
	public static void main(String[] args) {
		// 2D range query
		// modified skewness function and shaped parameter function

//		// 70%cell, from bit80 -> bit58
//		double[] x = { 1.19, 1.02, 0.40, 0.41, 0.25, 0.09};
//		double[] y = { 0.40, 0.40, 0.38, 0.38, 0.36, 0.28};

//		// 50%cell, from bit80 -> bit58
//		double[] x = { 1.50, 1.30, 0.52, 0.53, 0.32, 0.13};
//		double[] y = { 0.47, 0.48, 0.45, 0.45, 0.43, 0.33};

//		// 30%cell, from bit80 -> bit58
//		double[] x = { 1.80, 1.59, 0.70, 0.66, 0.43, 0.18};
//		double[] y = { 0.61, 0.62, 0.59, 0.59, 0.57, 0.46};

//		// 20%cell, from bit80 -> bit58
//		double[] x = { 2.15, 2.00, 0.81, 0.79, 0.54, 0.22};
//		double[] y = { 0.71, 0.72, 0.70, 0.70, 0.68, 0.59};

//		// modified bit03, from 10% to 200% cell
//		double[] x = {1.163, 0.792, 0.659, 0.59, 0.526, 0.458, 0.412, 0.365, 0.336, 0.3, 0.325, 0.317, 0.321, 0.305, 0.287};
//		double[] y = {0.833, 0.696, 0.591, 0.511, 0.445, 0.412, 0.376, 0.359, 0.341, 0.329, 0.3, 0.274, 0.251, 0.23, 0.21};
//		// modified bit70, from 10% to 200% cell
//		double[] x = { 1.164, 0.813, 0.699, 0.611, 0.52, 0.451, 0.405, 0.36, 0.327, 0.295, 0.321, 0.321, 0.32, 0.308,
//				0.284 };
//		double[] y = { 0.833, 0.696, 0.591, 0.511, 0.445, 0.412, 0.376, 0.359, 0.341, 0.329, 0.3, 0.274, 0.251, 0.23,
//				0.21 };
//		// modified bit02, from 10% to 200% cell
//		double[] x = { 3.537, 1.999, 1.587, 1.477, 1.303, 1.123, 1.02, 0.924, 0.866, 0.807, 0.855, 0.833, 0.852, 0.826,
//				0.767 };
//		double[] y = { 0.847, 0.721, 0.623, 0.545, 0.477, 0.441, 0.401, 0.381, 0.361, 0.346, 0.315, 0.289, 0.266, 0.245,
//				0.225 };
//		// modified bit08, from 10% to 200% cell
//		double[] x = { 3.174, 2.154, 1.802, 1.67, 1.496, 1.29, 1.189, 1.067, 0.975, 0.901, 0.962, 0.933, 0.962, 0.938,
//				0.836 };
//		double[] y = { 0.842, 0.712, 0.61, 0.531, 0.467, 0.434, 0.398, 0.378, 0.36, 0.344, 0.315, 0.288, 0.265, 0.244,
//				0.223 };
//		// modified bit65, from 10% to 200% cell
//		double[] x = {0.717, 0.536, 0.433, 0.377, 0.324, 0.278, 0.248, 0.22, 0.198, 0.174, 0.191, 0.194, 0.193, 0.186, 0.173};
//		double[] y = {0.823, 0.681, 0.572, 0.491, 0.426, 0.395, 0.36, 0.344, 0.327, 0.314, 0.285, 0.26, 0.236, 0.215, 0.195};
//		// modified bit58, from 10% to 200% cell
//		double[] x = { 0.305, 0.223, 0.183, 0.153, 0.13, 0.11, 0.094, 0.077, 0.065, 0.049, 0.061, 0.069, 0.073, 0.071,
//				0.069 };
//		double[] y = { 0.766, 0.587, 0.464, 0.385, 0.33, 0.308, 0.282, 0.268, 0.252, 0.239, 0.21, 0.184, 0.16, 0.142,
//				0.127 };

//		// mixed
//		double[] x = { 0.305, 0.223, 0.183, 0.153, 0.13, 0.11, 0.094, 0.077, 0.065, 0.049, 0.061, 0.069, 0.073, 0.071,
//				0.069, 0.717, 0.536, 0.433, 0.377, 0.324, 0.278, 0.248, 0.22, 0.198, 0.174, 0.191, 0.194, 0.193, 0.186,
//				0.173, 1.164, 0.813, 0.699, 0.611, 0.52, 0.451, 0.405, 0.36, 0.327, 0.295, 0.321, 0.321, 0.32, 0.308,
//				0.284, 1.163, 0.792, 0.659, 0.59, 0.526, 0.458, 0.412, 0.365, 0.336, 0.3, 0.325, 0.317, 0.321, 0.305,
//				0.287, 3.174, 2.154, 1.802, 1.67, 1.496, 1.29, 1.189, 1.067, 0.975, 0.901, 0.962, 0.933, 0.962, 0.938,
//				0.836, 3.537, 1.999, 1.587, 1.477, 1.303, 1.123, 1.02, 0.924, 0.866, 0.807, 0.855, 0.833, 0.852, 0.826,
//				0.767 };
//		double[] y = { 0.767, 0.588, 0.465, 0.386, 0.33, 0.309, 0.282, 0.269, 0.253, 0.24, 0.21, 0.184, 0.161, 0.143,
//				0.127, 0.797, 0.637, 0.521, 0.439, 0.377, 0.35, 0.32, 0.306, 0.29, 0.277, 0.248, 0.222, 0.198, 0.178,
//				0.16, 0.845, 0.718, 0.617, 0.539, 0.474, 0.438, 0.401, 0.382, 0.363, 0.35, 0.321, 0.296, 0.273, 0.252,
//				0.232, 0.845, 0.718, 0.617, 0.539, 0.474, 0.438, 0.401, 0.382, 0.363, 0.35, 0.321, 0.296, 0.273, 0.252,
//				0.232, 0.895, 0.805, 0.731, 0.668, 0.611, 0.575, 0.533, 0.507, 0.482, 0.464, 0.432, 0.405, 0.384, 0.363,
//				0.344, 0.898, 0.812, 0.74, 0.678, 0.619, 0.581, 0.536, 0.511, 0.484, 0.467, 0.432, 0.406, 0.385, 0.364,
//				0.346 };

//		// mixed less
//		double[] x = { 0.717, 0.536, 0.433, 0.377, 0.324, 0.278, 0.248, 0.22, 0.198, 0.174, 0.191, 0.194, 0.193, 0.186,
//				0.173, 1.164, 0.813, 0.699, 0.611, 0.52, 0.451, 0.405, 0.36, 0.327, 0.295, 0.321, 0.321, 0.32, 0.308,
//				0.284, 1.163, 0.792, 0.659, 0.59, 0.526, 0.458, 0.412, 0.365, 0.336, 0.3, 0.325, 0.317, 0.321, 0.305,
//				0.287, 3.174, 2.154, 1.802, 1.67, 1.496, 1.29, 1.189, 1.067, 0.975, 0.901, 0.962, 0.933, 0.962, 0.938,
//				0.836, 3.537, 1.999, 1.587, 1.477, 1.303, 1.123, 1.02, 0.924, 0.866, 0.807, 0.855, 0.833, 0.852, 0.826,
//				0.767 };
//		double[] y = { 0.797, 0.637, 0.521, 0.439, 0.377, 0.35, 0.32, 0.306, 0.29, 0.277, 0.248, 0.222, 0.198, 0.178,
//				0.16, 0.845, 0.718, 0.617, 0.539, 0.474, 0.438, 0.401, 0.382, 0.363, 0.35, 0.321, 0.296, 0.273, 0.252,
//				0.232, 0.845, 0.718, 0.617, 0.539, 0.474, 0.438, 0.401, 0.382, 0.363, 0.35, 0.321, 0.296, 0.273, 0.252,
//				0.232, 0.895, 0.805, 0.731, 0.668, 0.611, 0.575, 0.533, 0.507, 0.482, 0.464, 0.432, 0.405, 0.384, 0.363,
//				0.344, 0.898, 0.812, 0.74, 0.678, 0.619, 0.581, 0.536, 0.511, 0.484, 0.467, 0.432, 0.406, 0.385, 0.364,
//				0.346 };

		// mixed less
//		double[] x = { 0.717, 0.536, 0.433, 0.377, 0.324, 0.278, 0.248, 0.22, 0.198, 0.174, 0.191, 0.194, 0.193, 0.186,
//				0.173, 1.164, 0.813, 0.699, 0.611, 0.52, 0.451, 0.405, 0.36, 0.327, 0.295, 0.321, 0.321, 0.32, 0.308,
//				0.284, 3.174, 2.154, 1.802, 1.67, 1.496, 1.29, 1.189, 1.067, 0.975, 0.901, 0.962, 0.933, 0.962, 0.938,
//				0.836 };
//		double[] y = { 0.797, 0.637, 0.521, 0.439, 0.377, 0.35, 0.32, 0.306, 0.29, 0.277, 0.248, 0.222, 0.198, 0.178,
//				0.16, 0.845, 0.718, 0.617, 0.539, 0.474, 0.438, 0.401, 0.382, 0.363, 0.35, 0.321, 0.296, 0.273, 0.252,
//				0.232, 0.895, 0.805, 0.731, 0.668, 0.611, 0.575, 0.533, 0.507, 0.482, 0.464, 0.432, 0.405, 0.384, 0.363,
//				0.344 };
		
		double[] x = {977.556,1120.375,1207.127};
		double[] y = {1.587,0.827,0.853};

		double rho = spearmanCorrelation(x, y);
		System.out.println("Spearman Correlation: " + String.format("%.3f", rho));
	}
}
