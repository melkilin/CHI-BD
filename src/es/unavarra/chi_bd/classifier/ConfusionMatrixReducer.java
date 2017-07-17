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

package es.unavarra.chi_bd.classifier;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.IntArrayWritable;

/**
 * Reducer class used to sum received confusion matrices
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class ConfusionMatrixReducer extends Reducer<ByteWritable, IntArrayWritable,ByteWritable, IntArrayWritable> {

	private Iterator<IntArrayWritable> iterator;
	private IntArrayWritable currentClassRow, classRow; // Each reducer receives only the row of a given class
	private byte classIndex;
	
    @Override
    public void reduce(ByteWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        
    	iterator = values.iterator();
    	
    	// Sum confusion matrices
    	classRow = new IntArrayWritable(new int[Mediator.getNumClasses()]);
    	while (iterator.hasNext()){
    		currentClassRow = iterator.next();
    		for (classIndex = 0; classIndex < Mediator.getNumClasses(); classIndex++)
    				classRow.getData()[classIndex] = classRow.getData()[classIndex] 
    						+ currentClassRow.getData()[classIndex];
    	}
    	
    	context.write(key, classRow);
        
    }
    
    @Override
	protected void setup(Context context) throws InterruptedException, IOException{

		super.setup(context);
		
		// Read configuration
		try {
			Mediator.setConfiguration(context.getConfiguration());
			Mediator.readClassifierConfiguration();
		}
		catch(Exception e){
			System.err.println("\nHITS REDUCER: ERROR READING CONFIGURATION\n");
			e.printStackTrace();
		}
		
    }
    
}

