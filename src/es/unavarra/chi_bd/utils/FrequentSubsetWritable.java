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

package es.unavarra.chi_bd.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.WritableComparable;

/**
 * Represents a subset of antecedents
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class FrequentSubsetWritable implements WritableComparable<FrequentSubsetWritable>, Serializable {
    
	private byte[] antecedents; // Antecedents of this subset
	private int split; // Split of the rule that this subset belongs to
	
	/**
     * Default constructor
     */
    public FrequentSubsetWritable() {}
    
    /**
     * Constructs a new subset of antecedents
     * @param antecedents antecedents of this subset
     * @param split split of the rule that this subset belongs to
     */
    public FrequentSubsetWritable(byte[] antecedents, int split) {
        this.antecedents = antecedents;
        this.split = split;
    }

	/**
     * Constructs a copy
     * @param copy copy of the object to be copied
     */
    public FrequentSubsetWritable(FrequentSubsetWritable copy) {
        this.antecedents = new byte[copy.antecedents.length];
        for (int i = 0; i < this.antecedents.length; i++)
        	this.antecedents[i] = copy.antecedents[i];
        this.split = copy.split;
    }
    
    @Override
	public int compareTo(FrequentSubsetWritable o) {
    	
    	if (this.split > o.split)
    		return 1;
    	else if (this.split < o.split)
    		return -1;
    	else {
    	
	    	int i = 0;
	    	if (antecedents.length == o.getAntecedents().length){
				while (i < antecedents.length && antecedents[i]==o.getAntecedents()[i])
					i++;
				if (i >= antecedents.length)
					return 0;
			}
	    	else if (antecedents.length < o.getAntecedents().length){
				while (i < antecedents.length && antecedents[i]==o.getAntecedents()[i])
					i++;
				if (i >= antecedents.length)
					return -1;
			}
			else{
				while (i < o.getAntecedents().length && antecedents[i]==o.getAntecedents()[i])
					i++;
				if (i >= o.getAntecedents().length)
					return 1;
			}
			if (antecedents[i] > o.getAntecedents()[i])
				return 1;
			else
				return -1;
		
    	}
    	
	}
    
    @Override
	public boolean equals (Object obj){
    	
    	if (obj == this)
    		return true;
    	if (obj == null || obj.getClass() != this.getClass())
    		return false;
    	
    	FrequentSubsetWritable o = (FrequentSubsetWritable)obj;
    	
    	// If the number of split is different then return false
    	if (this.split != o.split)
    		return false;
    	
    	// Check all the antecedents
    	int i = 0;
    	if (antecedents.length == o.getAntecedents().length){
			while (i < antecedents.length && antecedents[i]==o.getAntecedents()[i])
				i++;
			if (i >= antecedents.length)
				return true;
			else
				return false;
		}
    	else
    		return false;
    	
    }

    /**
     * Returns the antecedents of this subset
     * @return the antecedents of this subset
     */
    public byte[] getAntecedents() {
        return antecedents;
    }
    
    /**
     * Returns the split of the rule that this subset belongs to
     * @return the split of the rule that this subset belongs to
     */
    public int getNumSplit() {
    	return split;
    }
    
    @Override
    public int hashCode(){
    	return new String(antecedents+"s"+split).hashCode();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
    	
    	split = in.readInt();
    	
        int length = in.readInt();
        antecedents = new byte[length];

        for(int i = 0; i < length; i++)
            antecedents[i] = in.readByte();
        
    }

    /**
     * Sets the antecedents of this subset
     * @param antecedents the antecedents of this subset
     */
    public void setAntecedents(byte[] antecedents) {
        this.antecedents = antecedents;
    }

    @Override
    public String toString(){
    	
    	String output = "Antecedents (split "+split+"): ";
    	
    	for (int i = 0; i < antecedents.length; i++)
    		output += antecedents[i] + " | ";
    	
    	return output;
    	
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	
        int length = 0;
        
        if(antecedents != null)
            length = antecedents.length;

        out.writeInt(split);
        
        out.writeInt(length);

        for(int i = 0; i < length; i++)
            out.writeByte(antecedents[i]);
        
    }
    
}
