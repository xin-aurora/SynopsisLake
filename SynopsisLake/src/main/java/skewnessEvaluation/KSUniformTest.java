package skewnessEvaluation;

import java.util.Arrays;

public class KSUniformTest {

    /**
     * Main method to demonstrate how to compute the Kolmogorov–Smirnov
     * statistic (D) for testing if a sample comes from a uniform distribution.
     */
    public static void main(String[] args) {
        // Example data: Suppose we have an array of double values
        double[] sampleData = {2.1, 2.4, 3.0, 5.2, 5.3, 5.9, 7.8, 9.9, 10.0, 10.1};

        // Compute the K–S statistic for uniform distribution
        double ksStat = kolmogorovSmirnovUniform(sampleData);
        System.out.println("K–S statistic = " + ksStat);

        // Interpretation of ksStat requires additional steps or tables.
    }

    /**
     * Computes the Kolmogorov–Smirnov statistic (D) for testing
     * if the given sampleData might come from a Uniform(a, b) distribution.
     *
     * Here, 'a' and 'b' are estimated from the sample (min, max).
     *
     * K–S statistic D = max over i of:
     *   D+_i = (i / n)     - F(x_(i))
     *   D-_i = F(x_(i))    - ((i-1) / n)
     *
     * Often combined as D = max(D+_i, D-_i).
     *
     * @param sampleData array of observed data
     * @return Kolmogorov–Smirnov test statistic for Uniform(a, b)
     */
    public static double kolmogorovSmirnovUniform(double[] sampleData) {
        // Estimate a = min(data), b = max(data)
        double a = Double.POSITIVE_INFINITY;
        double b = Double.NEGATIVE_INFINITY;
        for (double value : sampleData) {
            if (value < a) a = value;
            if (value > b) b = value;
        }

        // Sort the data
        double[] sorted = Arrays.copyOf(sampleData, sampleData.length);
        Arrays.sort(sorted);

        int n = sorted.length;
        double dPlus = 0.0;
        double dMinus = 0.0;

        for (int i = 0; i < n; i++) {
            double x = sorted[i];
            double F = uniformCDF(x, a, b); 

            // Empirical CDF at rank i is (i+1)/n
            double empCDF = (i + 1) / (double) n;

            // D+ = max over i of (empCDF - F)
            double dPlusCandidate = empCDF - F;
            if (dPlusCandidate > dPlus) {
                dPlus = dPlusCandidate;
            }

            // D- = max over i of (F - i/n)
            double dMinusCandidate = F - (i / (double) n);
            if (dMinusCandidate > dMinus) {
                dMinus = dMinusCandidate;
            }
        }

        // The K–S statistic is the maximum of these two distances
        return Math.max(dPlus, dMinus);
    }

    /**
     * Uniform(a, b) CDF:
     *  0   if x < a
     *  1   if x > b
     *  (x-a)/(b-a) if a <= x <= b
     */
    private static double uniformCDF(double x, double a, double b) {
        if (x < a) {
            return 0.0;
        } else if (x > b) {
            return 1.0;
        } else {
            return (x - a) / (b - a);
        }
    }
}

