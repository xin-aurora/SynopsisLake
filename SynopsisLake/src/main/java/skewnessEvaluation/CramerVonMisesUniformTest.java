package skewnessEvaluation;

import java.util.Arrays;

public class CramerVonMisesUniformTest {

    public static void main(String[] args) {
        // Example data array
        double[] sampleData = {2.1, 2.4, 3.0, 5.2, 5.3, 5.9, 7.8, 9.9, 10.0, 10.1};

        // Compute the Cramér–von Mises statistic for uniform
        double cvmStat = cramerVonMisesUniformStatistic(sampleData);
        System.out.println("Cramér–von Mises statistic (W^2) for Uniform(a, b) = " + cvmStat);

        // Interpreting cvmStat requires further steps or tables (p-value computation).
    }

    /**
     * Computes the Cramér–von Mises (C–vM) statistic W^2 for testing if the
     * sampleData could come from a Uniform(a, b) distribution.
     *
     * We estimate 'a' and 'b' from the data as min and max, respectively.
     *
     * Formula:
     *   W^2 = 1/(12n) + sum_{i=1..n} [ F(x_(i)) - (2i - 1)/(2n) ]^2
     *
     * @param data Array of sample observations
     * @return The Cramér–von Mises statistic W^2
     */
    public static double cramerVonMisesUniformStatistic(double[] data) {
        if (data == null || data.length == 0) {
            return 0.0;
        }

        // 1. Estimate a = min(data), b = max(data)
        double a = Double.POSITIVE_INFINITY;
        double b = Double.NEGATIVE_INFINITY;
        for (double value : data) {
            if (value < a) a = value;
            if (value > b) b = value;
        }

        // 2. Sort the data
        double[] sorted = Arrays.copyOf(data, data.length);
        Arrays.sort(sorted);

        int n = sorted.length;
        double sum = 0.0;

        // 3. Compute the sum of squared differences
        for (int i = 0; i < n; i++) {
            // Theoretical CDF at x_(i)
            double F = uniformCDF(sorted[i], a, b);

            // Empirical fraction term: (2i - 1) / (2n),
            // using i+1 for 1-based index => (2(i+1) - 1)/(2n) = (2i+1)/(2n)
            double empFrac = (2.0 * (i + 1) - 1.0) / (2.0 * n);

            double diff = F - empFrac;
            sum += diff * diff;
        }

        // 4. Add the 1/(12n) term
        double w2 = (1.0 / (12.0 * n)) + sum;
        return w2;
    }

    /**
     * The CDF for Uniform(a, b):
     *   0   if x < a
     *   1   if x > b
     *   (x - a)/(b - a) if a <= x <= b
     */
    private static double uniformCDF(double x, double a, double b) {
        if (a >= b) {
            // Degenerate or invalid range
            return 0.0;
        }
        if (x <= a) {
            return 0.0;
        } else if (x >= b) {
            return 1.0;
        } else {
            return (x - a) / (b - a);
        }
    }
}
