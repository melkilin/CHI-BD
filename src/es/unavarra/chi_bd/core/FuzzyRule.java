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

import java.util.StringTokenizer;

import es.unavarra.chi_bd.utils.ByteArrayWritable;

/**
 * Provides the operations of fuzzy rules
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class FuzzyRule {
	
	private static int i, j, numSplit;
	private static int[] startIndex, endIndex;
	private static float matching;
	private static int freqSubsetMatchingIndex;
	private static byte[] ruleSplit;
    
    /**
     * Returns the matching degree of the input example with the specified antecedents. WARNING: the procedure setNumAntecedentsSubsets(int num) must be called before.
     * @param membershipDegrees pre-computed membership degrees
     * @param freqSubsetsMatching pre-computed matching degrees of the frequent antecedents subsets
     * @param splitsIndices indicates whether a split of a rule is a frequent subset or not, and its index (if it is frequent) in the matching table
     * @param ruleSplits splits of the rule
     * @param example input example
     * @return matching degree of the input example with the specified antecedents
     */
    public static float computeMatchingDegree (float[][] membershipDegrees, float[] freqSubsetsMatching, int[] splitsIndices, ByteArrayWritable[] ruleSplits, String[] example){
    	
    	matching = 1.0f;
    	
    	// Iterate over the pre-computed subsets of antecedents
        for (numSplit = 0; numSplit < Mediator.getNumRuleSplits() && matching > 0; numSplit++){
        	
        	// Check whether this subset is pre-computed
        	
        	freqSubsetMatchingIndex = splitsIndices[numSplit];
        	
        	if (freqSubsetMatchingIndex == -1){
 
        		// If this subset is not pre-computed, compute the matching degree of all the antecedents in this subset
        		ruleSplit = ruleSplits[numSplit].getBytes();
        		
        		j = 0;
        		for (i = startIndex[numSplit]; i <= endIndex[numSplit] && matching > 0; i++) {
        			
		        	if (Mediator.getVariables()[i] instanceof FuzzyVariable)
		        		matching *= membershipDegrees[i][ruleSplit[j]];
		        	// If it is a nominal value and it is not equal to the antecedent, then there is no matching
		        	else {
		        		if (!((NominalVariable)Mediator.getVariables()[i]).
		        				getNominalValue(ruleSplit[j]).contentEquals(example[i]))
		        			return 0.0f;
		        	}

		        	j++;
        		
        		}
        	
        	}
        	else 
        		matching *= freqSubsetsMatching[freqSubsetMatchingIndex];
        	
        }

        return matching;
    	
    }
    
    /**
     * Computes the membership degree of the input value to the specified fuzzy set
     * @param variable variable index
     * @param label linguistic label index
     * @param value input value
     * @return membership degree of the input value to the specified fuzzy set
     */
    public static float computeMembershipDegree (int variable, byte label, String value){
    	
    	if (Mediator.getVariables()[variable] instanceof NominalVariable){
    		if (!((NominalVariable)Mediator.getVariables()[variable]).
    				getNominalValue(label).contentEquals(value))
    			return 0.0f;
    		else
    			return 1.0f;
    	}
    	else
    		return (float)((FuzzyVariable)Mediator.getVariables()[variable]).getFuzzySets()[label].
    				computeMembershipDegree(Double.parseDouble(value));
    	
    }
    
    /**
     * Returns the partial matching degree of the input example with the specified subset of antecedents. WARNING: the procedure setNumAntecedentsSubsets(int num) must be called before.
     * @param membershipDegrees pre-computed membership degrees
     * @param split indicates the split of the rule
     * @param antecedents subset of antecedents of a rule
     * @param example input example
     * @return partial matching degree of the input example with the specified subset of antecedents
     */
    public static float computePartialMatchingDegree (float[][] membershipDegrees, int split, byte[] antecedents, String[] example){
    	
    	matching = 1.0f;
        
        // Compute matching degree
    	j = 0;
        for (i = startIndex[split]; i <= endIndex[split] && matching > 0; i++){
        	
        	if (Mediator.getVariables()[i] instanceof FuzzyVariable)
        		matching *= membershipDegrees[i][antecedents[j]];
        	// If it is a nominal value and it is not equal to the antecedent, then there is no matching
        	else {
        		if (!((NominalVariable)Mediator.getVariables()[i]).
        				getNominalValue(antecedents[j]).contentEquals(example[i]))
        			return 0.0f;
        	}
        	j++;
        }
        
        return matching;
    	
    }
    
    /**
     * Returns a new rule represented by a byte array containing the index of antecedents and the class index (at last position of the array)
     * @param exampleStr input string representing the example
     * @return a new rule represented by a byte array containing the index of antecedents and the class index (at last position of the array)
     */
    public static byte[] getRuleFromExample (String exampleStr){
    	
    	// Read values from string
        StringTokenizer st = new StringTokenizer(exampleStr, ", ");
        String [] inputValues = new String[st.countTokens()];
        int i = 0;
        while (st.hasMoreTokens()){
            inputValues[i] = st.nextToken();
            i++;
        }

        // Generate a new fuzzy rule
        Variable[] variables = Mediator.getVariables();
        byte[] labels = new byte[variables.length+1];
        // Get attributes
        for (i = 0; i < variables.length; i++)
        	labels[i] = variables[i].getLabelIndex(inputValues[i]);
        // Get the class
        labels[variables.length] = Mediator.getClassIndex(inputValues[variables.length]);
        
        return labels;
        
    }
    
    /**
     * Sets the number of splits in all rules. This procedure must be called to use pre-computed partial matching degrees.
     * @param num number of splits in all rules
     */
    public static void setNumSplits (){
        startIndex = new int[Mediator.getNumRuleSplits()];
        endIndex = new int[Mediator.getNumRuleSplits()];
        int[][] splitIndices = Mediator.getRuleSplitsIndices();
        for (numSplit = 0; numSplit < Mediator.getNumRuleSplits(); numSplit++){
        	startIndex[numSplit] = splitIndices[numSplit][0];
        	endIndex[numSplit] = splitIndices[numSplit][1];
        }
    }

}
