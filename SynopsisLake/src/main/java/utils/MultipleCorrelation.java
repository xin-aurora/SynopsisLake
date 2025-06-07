package utils;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class MultipleCorrelation {

    // Method to compute multiple correlation
    public static double computeMultipleCorrelation(double[] a, double[][] predictors) {
        // Ensure arrays have the correct dimensions
        if (a.length != predictors.length) {
            throw new IllegalArgumentException("The length of array a must match the number of rows in predictors.");
        }

        // Perform multiple linear regression
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.newSampleData(a, predictors);

        // Get R^2 (coefficient of determination)
        double rSquared = regression.calculateRSquared();

        // Return the square root of R^2 to get the multiple correlation coefficient
        return Math.sqrt(rSquared);
    }

    public static void main(String[] args) {
        // Example data
//        double[] a = {1, 2, 3, 4, 5}; // Dependent variable
//        double[] b = {2, 3, 4, 5, 6}; // Independent variable 1
//        double[] c = {5, 4, 3, 2, 1}; // Independent variable 2
    	double[] a = {0.107555763, 0.394072164, 0.34595835, 0.227840292};
    	double[] b = {1.257696514, 1.257696514, 1.257696514, 1.257696514};
    	double[] c = {0.500263774, 0.400113084, 0.300065178, 0.200113499};

        // Combine b and c into a matrix of predictors
        double[][] predictors = new double[b.length][2];
        for (int i = 0; i < b.length; i++) {
            predictors[i][0] = b[i];
            predictors[i][1] = c[i];
        }

        try {
            // Compute multiple correlation
            double multipleCorrelation = computeMultipleCorrelation(a, predictors);
            System.out.printf("Multiple correlation of a with {b, c}: %.4f%n", multipleCorrelation);
        } catch (IllegalArgumentException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}


