package skewnessEvaluation;

import java.util.Arrays;

public class AndersonDarlingUniformTest {

    public static void main(String[] args) {
        // Example data that might or might not be uniform on some interval
        double[] sampleData = {2.1, 2.4, 3.0, 5.2, 5.3, 5.9, 7.8, 9.9, 10.0, 10.1};

        // Compute the A–D statistic for Uniform(a, b)
        double adStat = andersonDarlingUniformStatistic(sampleData);
        System.out.println("Anderson–Darling statistic for Uniform(a, b) = " + adStat);

        // Interpreting adStat requires further steps or a table for p-values.
    }

    /**
     * Computes the Anderson–Darling test statistic A^2 for testing if the
     * sampleData could come from a Uniform(a, b) distribution.
     *
     * We estimate 'a' and 'b' from the data as min and max, respectively.
     *
     * Formula (generic for a known CDF F):
     *   A^2 = -n - (1/n) * SUM_{i=1..n} [
     *              (2i - 1)*ln(F(x_(i))) +
     *              (2(n-i)+1)*ln(1 - F(x_(n+1-i)))
     *          ]
     *
     * @param data Array of sample observations
     * @return The Anderson–Darling statistic A^2
     */
    public static double andersonDarlingUniformStatistic(double[] data) {
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

        // 3. Anderson–Darling sum
        for (int i = 0; i < n; i++) {
            // x_(i) from the lower end
            double xLow  = sorted[i];
            // x_(n - 1 - i) from the upper end
            double xHigh = sorted[n - 1 - i];

            double F1 = uniformCDF(xLow, a, b);
            double F2 = 1.0 - uniformCDF(xHigh, a, b);

            // Prevent log(0)
            if (F1 <= 0.0) F1 = 1e-16;
            if (F2 <= 0.0) F2 = 1e-16;

            // (2i - 1)*ln(F(x_(i))) + [2(n-i)+1]*ln(1 - F(x_(n+1-i)))
            // but note x_(n+1 - i) is x_(n - i) in 0-based indexing => xHigh
            double term = (2.0 * (i + 1) - 1.0) * Math.log(F1)
                        + (2.0 * (n - (i + 1)) + 1.0) * Math.log(F2);
            sum += term;
        }

        double a2 = -n - (sum / n);
        return a2;
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

