/*
 * Copyright (C) 2014 Mikel Elkano Ilintxeta
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package es.unavarra.chi_bd.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import es.unavarra.chi_bd.utils.ByteArrayWritable;

/**
 * Represents a rule base
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class RuleBase {
	
	public static final byte FRM_WINNING_RULE = 0;
	public static final byte FRM_ADDITIVE_COMBINATION = 1;
    
    /**
     * Rule base
     */
	private int numRules; // Number of rules
    private ByteArrayWritable[][] rulesSplits; // Splitted rules
    private float[] rulesWeights; // Weights of splitted rules
    private byte[] rulesClasses; // Classes of the rules
    private int[][] splitsIndices; // Indicates whether a split of a rule is a frequent subset or not, and its index (if it is frequent) in the matching degrees table
    private float[] freqSubsetsMatching; // Stores the matching degrees of the most frequent subsets of antecedents for a given example
    private HashMap<ByteArrayWritable,Integer>[] freqSubsetsIndices; // Index of each frequent subset in the matching degrees table
    float[][] membershipDegrees; // Pre-computed membership degrees of a given example
    
    /**
     * Temporary structures
     */
    private Iterator<Entry<ByteArrayWritable, Integer>> freqSubsetsIterator; // Iterates over the hash map
	private Entry<ByteArrayWritable, Integer> freqSubsetEntry; // Hash Map entry
    private int i;
    private byte label;
    
    /**
     * Constructs a new rule base with the given splitted rules
     * @param rulesSplits splits of the rules
     * @param rulesWeights weights of the splitted rules
     * @param rulesClasses classes of the rules
     * @param splitsIndices indicates whether a split of a rule is a frequent subset or not, and its index (if it is frequent) in the matching table
     * @param freqSubsetsIndices index of each frequent subset in the matching degrees table
     * @param numFreqSubsets number of frequent subsets
     */
    public RuleBase (ByteArrayWritable[][] rulesSplits, float[] rulesWeights, byte[] rulesClasses, int[][] splitsIndices, HashMap<ByteArrayWritable,Integer>[] freqSubsetsIndices, int numFreqSubsets){
    	this.rulesSplits = rulesSplits;
    	this.rulesWeights = rulesWeights;
    	this.rulesClasses = rulesClasses;
    	this.splitsIndices = splitsIndices;
    	this.freqSubsetsIndices = freqSubsetsIndices;
    	
    	this.freqSubsetsMatching = new float[numFreqSubsets];
    	this.membershipDegrees = new float[Mediator.getNumVariables()][Mediator.getNumLinguisticLabels()]; // Pre-computed membership degrees
    	
    	this.numRules = rulesClasses.length;
    	FuzzyRule.setNumSplits();
    }
    
    /**
     * Classifies an example
     * @param frm fuzzy reasoning method to be used (0: winning rule, 1: additive combination)
     * @param example input example
     * @return predicted class
     */
    public byte classify (byte frm, String[] example){
    	if (frm == FRM_WINNING_RULE)
    		return (byte)FRM_WR(example)[0];
    	else
    		return (byte)FRM_AC(example)[0];
    }
    
    /**
     * Additive Combination Fuzzy Reasoning Method
     * @param example input example
     * @return a double array where [0] is the predicted class index and [1] is the confidence degree
     */
    private double[] FRM_AC (String[] example){
    	
    	double[] output = new double[2];
    	output[0] = Mediator.getMostFrequentClass(); // Default class
    	output[1] = 0.0; // Default confidence
    	
    	double[] classDegree = new double[Mediator.getNumClasses()];
    	for (byte i = 0; i < classDegree.length; i++) classDegree[i] = 0.0;
    	
    	// Pre-compute membership degrees
    	for (i = 0; i < example.length; i++)
	    	if (Mediator.getVariables()[i] instanceof FuzzyVariable)
				for (label = 0; label < Mediator.getNumLinguisticLabels(); label++)
					membershipDegrees[i][label] = FuzzyRule.computeMembershipDegree(i,label,example[i]);
    	
    	// Pre-compute the matching degree with the most frequent subsets
		for (i = 0; i < Mediator.getNumRuleSplits(); i++){
			freqSubsetsIterator = freqSubsetsIndices[i].entrySet().iterator();
	        while (freqSubsetsIterator.hasNext()){
	        	freqSubsetEntry = freqSubsetsIterator.next();
	        	freqSubsetsMatching[freqSubsetEntry.getValue()] = 
	      			FuzzyRule.computePartialMatchingDegree(
					membershipDegrees, i, freqSubsetEntry.getKey().getBytes(), example);
	        }
	    }

    	// Compute the confidence of each class
		for (i = 0; i < numRules; i++)
    		classDegree[rulesClasses[i]] += FuzzyRule.computeMatchingDegree(
				membershipDegrees, freqSubsetsMatching, splitsIndices[i], 
				rulesSplits[i], example) * rulesWeights[i];
    	
    	// Get the class with the highest confidence
    	for (byte i = 0; i < classDegree.length; i++) {
    		if (classDegree[i] < 0){
    			System.err.println("\nERROR: One of the input values is out of the variable's range.\n\nABORTED\n");
    			System.exit(-1);
    		}
    		if (classDegree[i] > output[1]) {
    			output[0] = i;
    			output[1] = classDegree[i];
    		}
    	}
    	
    	return output;
    	
    }
    
    /**
     * Winning Rule Fuzzy Reasoning Method
     * @param example input example
     * @return a double array where [0] is the predicted class index and [1] is the confidence degree
     */
    private double[] FRM_WR (String[] example){
    	
    	double[] output = new double[2];
    	output[0] = Mediator.getMostFrequentClass(); // Default class
    	output[1] = 0.0; // Default confidence
    	
    	double degree;
    	
    	// Pre-compute membership degrees
    	for (i = 0; i < example.length; i++)
	    	if (Mediator.getVariables()[i] instanceof FuzzyVariable)
				for (label = 0; label < Mediator.getNumLinguisticLabels(); label++)
					membershipDegrees[i][label] = FuzzyRule.computeMembershipDegree(i,label,example[i]);
    	
    	// Pre-compute the matching degree with the most frequent subsets
		for (i = 0; i < Mediator.getNumRuleSplits(); i++){
			freqSubsetsIterator = freqSubsetsIndices[i].entrySet().iterator();
	        while (freqSubsetsIterator.hasNext()){
	        	freqSubsetEntry = freqSubsetsIterator.next();
	        	freqSubsetsMatching[freqSubsetEntry.getValue()] = 
	      			FuzzyRule.computePartialMatchingDegree(
					membershipDegrees, i, freqSubsetEntry.getKey().getBytes(), example);
	        }
	    }
    	
    	// Compute the confidence of each class
		for (i = 0; i < numRules; i++){
    		degree = FuzzyRule.computeMatchingDegree(
				membershipDegrees, freqSubsetsMatching, splitsIndices[i], 
				rulesSplits[i], example) * rulesWeights[i];
    		if (degree < 0){
    			System.err.println("\nERROR: One of the input values is out of the variable's range.\n\nABORTED\n");
    			System.exit(-1);
    		}
    		if (degree > output[1]){
    			output[0] = rulesClasses[i];
    			output[1] = degree;
    		}
		}
    	
    	return output;
    	
    }

}
