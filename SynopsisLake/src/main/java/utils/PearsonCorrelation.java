package utils;

public class PearsonCorrelation {
	/**
	 * Computes the Pearson correlation coefficient between two arrays.
	 *
	 * @param x the first array of values
	 * @param y the second array of values
	 * @return Pearson's correlation coefficient
	 * @throws IllegalArgumentException if the arrays have different lengths or are
	 *                                  empty
	 */
	public static double pearsonCorrelation(double[] x, double[] y) {
		if (x == null || y == null || x.length == 0 || y.length == 0) {
			throw new IllegalArgumentException("Input arrays cannot be null or empty.");
		}
		if (x.length != y.length) {
			throw new IllegalArgumentException("Input arrays must have the same length.");
		}

		// 1. Compute the means of x and y
		double meanX = mean(x);
		double meanY = mean(y);

		// 2. Compute the numerator and the denominators
		double numerator = 0.0;
		double sumSqX = 0.0; // sum of squared deviations of x
		double sumSqY = 0.0; // sum of squared deviations of y

		for (int i = 0; i < x.length; i++) {
			double dx = x[i] - meanX;
			double dy = y[i] - meanY;

			numerator += (dx * dy);
			sumSqX += (dx * dx);
			sumSqY += (dy * dy);
		}

		// 3. Combine the results
		// If either sumSqX or sumSqY is 0, correlation is undefined (or 0 if all values
		// are the same).
		double denominator = Math.sqrt(sumSqX) * Math.sqrt(sumSqY);
		if (denominator == 0) {
			// This means the data in either x or y (or both) are all the same number
			// Technically correlation is undefined, but often handled as 0.
			return 0.0;
		}

		return numerator / denominator;
	}

	/**
	 * Helper function to compute the mean of an array.
	 *
	 * @param values the array of values
	 * @return the mean
	 */
	private static double mean(double[] values) {
		double sum = 0.0;
		for (double v : values) {
			sum += v;
		}
		return sum / values.length;
	}

	// --- Test the code ---
	public static void main(String[] args) {
		// modified function
//		// modified bit58, from 5%cell to 25% cell
//		double[] x = {0.4874616587842628, 0.3366406751173381, 0.24307769036433652, 0.18935128193037862};
//		double[] y = {36.47175104812797, 17.870497698614308, 10.761032348389623, 7.139206079217916};
//		// modified bit70, from 5% to 25% cell
//		double[] x = { 1.4448385777797579, 1.1953395840469851, 0.8995065735359812, 0.6999986718012992};
//		double[] y = { 38.73787050822165, 19.58310826323162, 12.231848621730634, 8.480934354073499};
//		// 5%cell, from bit80 -> bit58
//		double[] x = { 5.290541660211167, 1.4448385777797579, 1.003880941438387, 0.4874616587842628 };
//		double[] y = { 39.09615432175435, 38.73787050822165, 37.55274357514295, 36.47175104812797 };
//		// 25%cell, from bit58 -> bit80
//		double[] x = { 0.18935128193037862, 0.4543248948643367, 0.6999986718012992, 1.7347789119191614};
//		double[] y = { 7.139206079217916, 8.044018531593107, 8.480934354073499, 9.027098083265848};

//    	// bit 58
//    	double[] x = { 0.4874616587842628, 0.3366406751173381, 0.24307769036433652, 0.18935128193037862};
//    	// bit 70
//    	double[] x = { 1.4448385777797579, 1.1953395840469851, 0.8995065735359812, 0.6999986718012992};
//    	// bit 80
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
		
		double[] x = { 0.717, 0.536, 0.433, 0.377, 0.324, 0.278, 0.248, 0.22, 0.198, 0.174, 0.191, 0.194, 0.193, 0.186,
				0.173, 1.164, 0.813, 0.699, 0.611, 0.52, 0.451, 0.405, 0.36, 0.327, 0.295, 0.321, 0.321, 0.32, 0.308,
				0.284, 3.174, 2.154, 1.802, 1.67, 1.496, 1.29, 1.189, 1.067, 0.975, 0.901, 0.962, 0.933, 0.962, 0.938,
				0.836 };
		double[] y = { 0.797, 0.637, 0.521, 0.439, 0.377, 0.35, 0.32, 0.306, 0.29, 0.277, 0.248, 0.222, 0.198, 0.178,
				0.16, 0.845, 0.718, 0.617, 0.539, 0.474, 0.438, 0.401, 0.382, 0.363, 0.35, 0.321, 0.296, 0.273, 0.252,
				0.232, 0.895, 0.805, 0.731, 0.668, 0.611, 0.575, 0.533, 0.507, 0.482, 0.464, 0.432, 0.405, 0.384, 0.363,
				0.344 };
		
//		double[] x = {1.163, 0.792, 0.659, 0.59, 0.526, 0.458, 0.412, 0.365, 0.336, 0.3, 0.325, 0.317, 0.321, 0.305, 0.287};
//		double[] y = {0.833, 0.696, 0.591, 0.511, 0.445, 0.412, 0.376, 0.359, 0.341, 0.329, 0.3, 0.274, 0.251, 0.23, 0.21};

		double r = pearsonCorrelation(x, y);
		System.out.println("Pearson Correlation: " + String.format("%.3f", r));
	}
}
