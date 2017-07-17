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

/**
 * Represents a fuzzy variable of the problem, containing <i>l</i> linguistic labels (fuzzy sets), being <i>l</i> the number of linguistic labels specified by the user
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class FuzzyVariable extends Variable {
	
	/*
	 * If you are analyzing a high dimensional dataset consider using float type variables
	 * instead of doubles in order to save memory.
	 */
    
    /**
     * Fuzzy sets that compose this fuzzy variable
     */
    private FuzzySet[] fuzzySets;
    
    /**
     * Merge points of fuzzy sets
     */
    private double[] mergePoints;
    
    /**
     * Creates a new fuzzy variable
     * @param name variable name
     */
    public FuzzyVariable (String name){
    	
    	super(name);
    	
    }
    
    /**
     * Builds the fuzzy sets (modeled by triangular membership functions) that compose this fuzzy variable from the input limits 
     * @param lowerLimits lower limit of each variable
     * @param upperLimits upper limit of each variable
     * @param numLinguisticLabels number of linguistic labels (fuzzy sets) that compose this variable
     */
    public void buildFuzzySets (double lowerLimit, double upperLimit, byte numLinguisticLabels){
        
        fuzzySets = new FuzzySet[numLinguisticLabels];
        mergePoints = new double[numLinguisticLabels-1];
        
        /***** Ruspini partitioning *****/
        
        for (byte label = 0; label < numLinguisticLabels; label++){
        
            // Compute the half of the triangle's base
            double halfBase = (upperLimit - lowerLimit) / (numLinguisticLabels - 1);
            
            // We add the half of the triangle's base n times to the lower limit,
            // depending on the linguistic label and the point we want to obtain (left, mid, right)
            double leftPoint = lowerLimit + halfBase * (label - 1);
            double midPoint = lowerLimit + halfBase * (label);
            double rightPoint = lowerLimit + halfBase * (label + 1);
            if (label == 0)
                leftPoint = midPoint;
            else if (label == numLinguisticLabels - 1)
                rightPoint = midPoint;
            
            // We create the fuzzy set
            fuzzySets[label] = new FuzzySet(leftPoint, midPoint, rightPoint, label);
            
            // We add the merge point
            if (label > 0)
                mergePoints[label-1] = midPoint - ((midPoint - fuzzySets[label-1].getMidPoint()) / 2);
                
        }
        
    }
    
    /**
     * Returns the fuzzy sets that compose this fuzzy variable
     * @return fuzzy sets that compose this fuzzy variable
     */
    public FuzzySet[] getFuzzySets (){
        return fuzzySets;
    }
    
    /**
     * Returns the variable label index corresponding to the input value
     * @param inputValue input value
     * @return Variable label index corresponding to the input value
     */
    @Override
	public byte getLabelIndex(String inputValue){
    	return getMaxMembershipFuzzySet (Double.parseDouble(inputValue));
    }
    
    /**
     * Returns the index of the fuzzy set with the highest membership degree for the input value
     * @param value input value
     * @return index of the fuzzy set that returns the highest membership degree for the input value
     */
    private byte getMaxMembershipFuzzySet (double value){
        
        byte index = -1;
        
        // Since this function is used only in the learning stage,
        // we do not compute membership degrees. Instead, we check
        // the location of the input value with respect to the point
        // where two fuzzy sets merge.
        for (byte i = 0; i < mergePoints.length; i++){
            if (value < mergePoints[i]){
                index = i;
                break;
            }
        }
        if (index < 0)
            index = (byte)(mergePoints.length);
        
        return index;
        
    }
    
    @Override
    public String toString (){
        
        String output = getName() + ":\n";
        
        for (byte i = 0; i < fuzzySets.length; i++)
            output += " L_"+i+": ("+fuzzySets[i].getLeftPoint()+","+fuzzySets[i].getMidPoint()+","+fuzzySets[i].getRightPoint()+")\n";
        
        return output;
        
    }

}
