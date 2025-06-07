package utils;

import java.util.Arrays;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.special.Gamma;

public class BetaHealper {
    public static double betaFunction(double x, double y) {
        return Math.exp(Gamma.logGamma(x) + Gamma.logGamma(y) - Gamma.logGamma(x + y));
    }

    public static void main(String[] args) {
        double alpha = 5; // Alpha parameter
        double beta = 45; // Beta parameter
        double x = 0.5;
        BetaDistribution betaDistribution = new BetaDistribution(alpha, beta);
//        double pdf = betaDistribution.density(x);
//        System.out.println("PDF at x=" + x + ": " + pdf);

//		double betaFunc = betaFunction(alpha, beta);
//		
		double[] input = new double[100];
		double[] values = new double[100];
		for (int i=0; i<input.length; i++) {
			double factor = i / 100.0;
			values[i] = betaDistribution.density(factor);
			System.out.println(values[i]);
		}
//		System.out.println(Arrays.toString(values));
//        System.out.println("Beta function for alpha=" + alpha + ", beta=" + beta + ": " + betaFunc);
    }
}
