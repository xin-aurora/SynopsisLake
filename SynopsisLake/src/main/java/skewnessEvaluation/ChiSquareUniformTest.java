package skewnessEvaluation;

import java.util.Arrays;

public class ChiSquareUniformTest {

    public static void main(String[] args) {
        // Example data
        double[] sampleData = {2.1, 2.4, 3.0, 5.2, 5.3, 5.9, 7.8, 9.9, 10.0, 10.1};

        // Choose how many bins to use
        int numberOfBins = sampleData.length; // You can adjust this

        // Compute Chi-square statistic
        double chiSquareStat = chiSquareTestUniform(sampleData, numberOfBins);
        System.out.println("Chi-square statistic (Uniform) = " + chiSquareStat);

        // Interpreting chiSquareStat requires additional steps (p-value calculation).
    }

    /**
     * Computes the Chi-square statistic for how far the data deviate from
     * a Uniform(a, b) distribution (where a and b are estimated from the data).
     *
     * Steps:
     * 1) Estimate a = min(data), b = max(data)
     * 2) Divide [a, b] into k equal-width bins
     * 3) Count how many points (observed_i) fall in each bin
     * 4) Expected frequency for each bin (expected_i) = totalCount / k
     *    [assuming a perfect uniform distribution across those bins]
     * 5) Chi-square = sum( (obs_i - exp_i)^2 / exp_i ) over all bins
     *
     * @param data         the array of data
     * @param numberOfBins how many bins to divide [a,b] into
     * @return the Chi-square statistic
     */
    public static double chiSquareTestUniform(double[] data, int numberOfBins) {
        if (data == null || data.length == 0) {
            return 0.0;
        }

        // 1. Estimate a and b
        double a = Arrays.stream(data).min().getAsDouble();
        double b = Arrays.stream(data).max().getAsDouble();

        // Edge case: if a == b, the data are all the same point
        if (a == b) {
            // The distribution is degenerate; no real "uniform range" to test.
            return 0.0;
        }

        // 2. Bin edges (equal width)
        double range = b - a;
        double binWidth = range / numberOfBins;

        // Observed frequencies
        int[] observed = new int[numberOfBins];
        Arrays.fill(observed, 0);

        // 3. Count how many points fall into each bin
        for (double value : data) {
            // Determine which bin this value goes into
            // bin index = floor((value - a) / binWidth)
            // but handle the edge case if value == b (to avoid index = numberOfBins)
            int binIndex = (int) Math.floor((value - a) / binWidth);
            if (binIndex == numberOfBins) {
                binIndex = numberOfBins - 1; // put on the last bin if it hits the upper boundary
            }
            observed[binIndex]++;
        }

        // 4. Theoretical distribution is uniform => expected frequency is totalCount/k
        double n = data.length;
        double expectedPerBin = n / (double) numberOfBins;

        // 5. Compute Chi-square statistic
        double chiSquare = 0.0;
        for (int i = 0; i < numberOfBins; i++) {
            double diff = observed[i] - expectedPerBin;
            chiSquare += (diff * diff) / expectedPerBin;
        }

        return chiSquare;
    }
}
