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

package es.unavarra.chi_bd.learner.stage1;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_bd.utils.ByteArrayWritable;

/**
 * Reducer class used to gather rules
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class RulesGenerationReducer extends Reducer<ByteArrayWritable, ByteArrayWritable,ByteArrayWritable, ByteArrayWritable> {

	private Iterator<ByteArrayWritable> iterator;
	private HashSet<Byte> aggregatedBytesTmp;
	private ByteArrayWritable currentBytes;
	private byte[] aggregatedBytes;
	private int i;
	
    @Override
    public void reduce(ByteArrayWritable key, Iterable<ByteArrayWritable> values, Context context) throws IOException, InterruptedException {
        
    	iterator = values.iterator(); // class labels
    	
    	// Put all class labels into a single array
    	aggregatedBytesTmp = new HashSet<Byte>();
    	while (iterator.hasNext()){
    		currentBytes = iterator.next();
    		for (byte currentByte:currentBytes.getBytes())
    				aggregatedBytesTmp.add(currentByte);
    	}
    	aggregatedBytes = new byte[aggregatedBytesTmp.size()];
    	i = 0;
    	for (Byte currentByte:aggregatedBytesTmp) {
    		aggregatedBytes[i] = currentByte.byteValue();
    		i++;
    	}
    	
    	/*
    	 * Key: Antecedents of the rule
    	 * Value: Classes of the rule (conflicts)
    	 */
    	context.write(key, new ByteArrayWritable(aggregatedBytes));
        
    }
    
}

