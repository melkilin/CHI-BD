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

import java.io.Serializable;

/**
 * Represents a variable of the problem
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public abstract class Variable implements Serializable {
    
    /**
     * Variable name
     */
    private String name;
    
    /**
     * Creates a new variable
     * @param name variable name
     */
    protected Variable (String name){
    	
    	this.name = name;
    	
    }
    
    /**
     * Returns the variable label index corresponding to the input value
     * @param inputValue input value
     * @return Variable label index corresponding to the input value
     */
    public abstract byte getLabelIndex(String inputValue);
    
    /**
     * Returns the variable name
     * @return variable name
     */
    public String getName (){
        return name;
    }

}
