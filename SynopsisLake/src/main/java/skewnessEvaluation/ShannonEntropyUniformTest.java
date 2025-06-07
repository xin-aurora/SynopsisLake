package skewnessEvaluation;

import java.util.HashMap;
import java.util.Map;

import java.util.Arrays;

public class ShannonEntropyUniformTest {

    public static void main(String[] args) {
        // Example data
        double[] sampleData = {2.1, 2.4, 3.0, 5.2, 5.3, 5.9, 7.8, 9.9, 10.0, 10.1};

        // Number of bins for discretization
        int numberOfBins = sampleData.length; // Adjust as needed

        // Compute Shannon entropy for the data binned across [a, b]
        double entropy = shannonEntropyUniform(sampleData, numberOfBins);
        System.out.println("Shannon Entropy (bits) = " + entropy);

        // The maximum possible entropy for 'numberOfBins' bins is log2(numberOfBins)
        double maxEntropy = log2(numberOfBins);
        System.out.println("Max possible entropy for " + numberOfBins + " bins = " + maxEntropy);

        // A simple metric: how much less than the maximum is the sample's entropy
        double difference = maxEntropy - entropy;
        System.out.printf("Difference from max entropy = %.4f bits\n", difference);

        // Interpretation:
        // - difference ~ 0 => data is close to uniformly spread among the bins
        // - difference > 0 => data is more "peaked" or "clustered" than a uniform distribution
    }

    /**
     * Estimates [a, b] from the data (min, max),
     * bins the data into 'numberOfBins' bins of equal width,
     * and computes the Shannon entropy in bits.
     *
     * Steps:
     *  1) Find min (a) and max (b) in data.
     *  2) Divide [a,b] into 'numberOfBins' bins of equal width.
     *  3) Count how many data points fall into each bin => freq[i].
     *  4) p_i = freq[i] / data.length => discrete distribution across bins.
     *  5) Shannon entropy = - SUM(p_i * log2(p_i)) over non-empty bins.
     *
     * @param data          the sample data
     * @param numberOfBins  how many bins to use
     * @return Shannon entropy in bits
     */
    public static double shannonEntropyUniform(double[] data, int numberOfBins) {
        if (data == null || data.length == 0 || numberOfBins <= 1) {
            return 0.0;
        }

        // 1) Estimate a (min) and b (max)
        double a = Arrays.stream(data).min().getAsDouble();
        double b = Arrays.stream(data).max().getAsDouble();

        if (a == b) {
            // All data points are identical => zero entropy
            return 0.0;
        }

        double range = b - a;
        double binWidth = range / numberOfBins;

        // 2) Initialize bin counts
        int[] freq = new int[numberOfBins];
        Arrays.fill(freq, 0);

        // 3) Assign each data point to a bin
        for (double value : data) {
            int binIndex = (int) Math.floor((value - a) / binWidth);

            // Handle the edge case where value == b (avoid binIndex = numberOfBins)
            if (binIndex == numberOfBins) {
                binIndex = numberOfBins - 1;
            }
            freq[binIndex]++;
        }

        // 4) Convert counts to probabilities
        double n = data.length;
        double entropy = 0.0;
        for (int count : freq) {
            if (count > 0) {
                double p = count / n;
                entropy += p * log2(p);
            }
        }

        // 5) Shannon entropy = - SUM( p_i * log2(p_i) )
        entropy = -entropy;
        return entropy;
    }

    /**
     * Helper function to compute log base 2
     */
    private static double log2(double x) {
        return Math.log(x) / Math.log(2.0);
    }
}
