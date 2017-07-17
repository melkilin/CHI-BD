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

package es.unavarra.chi_bd.learner.stage2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.FrequentSubsetWritable;

/**
 * Reducer class that adds the number of occurrences of a given subset
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class FrequentSubsetsReducer extends Reducer<FrequentSubsetWritable, LongWritable, FrequentSubsetWritable, LongWritable> {

	private Iterator<LongWritable> valueIterator;
	private long count;
	
    @Override
    public void reduce(FrequentSubsetWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        
    	valueIterator = values.iterator();
    	
    	count = 0;
    	while (valueIterator.hasNext())
    		count += valueIterator.next().get();
    	
    	System.out.println(key);
    	if (count >= Mediator.getMinFreqSubsetOccurrence())
    		context.write(key,new LongWritable(count));
        
    }
    
    @Override
	protected void setup(Context context) throws InterruptedException, IOException{

		super.setup(context);
		
		try {
			Mediator.setConfiguration(context.getConfiguration());
			Mediator.readLearnerConfiguration();
		}
		catch(Exception e){
			System.err.println("\nSTAGE 2: ERROR READING CONFIGURATION\n");
			e.printStackTrace();
			System.exit(-1);
		}
		
    }
    
}
