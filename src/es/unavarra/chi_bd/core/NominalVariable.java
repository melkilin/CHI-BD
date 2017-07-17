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
 * Represents a nominal variable of the problem
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class NominalVariable extends Variable {
    
    /**
     * Nominal values of this variable (if it is a nominal variable)
     */
    private String[] nominalValues;
    
    /**
     * Creates a new nominal variable
     * @param name variable name
     */
    public NominalVariable (String name){
    	
    	super(name);
    	
    }
    
    /**
     * Returns the position of the input nominal value
     * @param nominalValue input nominal value
     * @return position of the input nominal value
     */
    private byte getIndexOfNominalValue (String nominalValue){
        byte index = -1;
        for (byte i = 0; i < nominalValues.length; i++)
        	if (nominalValues[i].contentEquals(nominalValue)){
        		index = i;
        		break;
        	}
        return index;
    }
    
    /**
     * Returns the variable label index corresponding to the input value
     * @param inputValue input value
     * @return Variable label index corresponding to the input value
     */
    @Override
	public byte getLabelIndex(String inputValue){
    	return getIndexOfNominalValue (inputValue);
    }
    
    /**
     * Returns the nominal value at the specified position
     * @param index position of nominal value
     * @return nominal value at the specified position
     */
    public String getNominalValue (byte index){
        return nominalValues[index];
    }
    
    /**
     * Returns the nominal values that compose this variable
     * @return nominal values that compose this variable
     */
    public String[] getNominalValues (){
        return nominalValues;
    }
    
    /**
     * Sets nominal values
     * @param nominalValues input nominal values
     */
    public void setNominalValues (String[] nominalValues){
        this.nominalValues = nominalValues;
    }
    
    @Override
    public String toString (){
        
        String output = getName() + " (nominal variable):\nNominal values: ";
        
        for (byte i = 0; i < nominalValues.length-1; i++)
            output += nominalValues[i]+", ";
        output += nominalValues[nominalValues.length-1]+"\n";
        
        return output;
        
    }

}
