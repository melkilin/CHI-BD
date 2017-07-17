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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import es.unavarra.chi_bd.core.Mediator;
import es.unavarra.chi_bd.utils.IntArrayWritable;

/**
 * Launches MapReduce to classify input examples. Usage: hadoop jar <jar_file> es.unavarra.chi_bd.classifier.mapreduce.MapReduceLauncher [-p] [-v] <hdfs://url:port> <parameters_file> <header_file> <input_database_path> <input_rulebase_path> <input_path> <output_path>
 * @author Mikel Elkano Ilintxeta
 * @version 1.0
 */
public class MapReduceLauncher {
	
	private static final char[] VALID_OPTIONAL_ARGS = new char[]{};
    private static final int MAX_OPTIONAL_ARGS = VALID_OPTIONAL_ARGS.length;
    private static final int NUM_PROGRAM_ARGS = 7;
    private static final String COMMAND_STR = "hadoop jar <jar_file> es.unavarra.chi_bd.classifier.MapReduceLauncher"+printOptionalArgs()+"<hdfs://url:port> <parameters_file> <header_file> <input_database_path> <input_rulebase_path> <input_path> <output_path>";
    
    /**
     * Returns true if the specified arguments contains a given argument
     * @param args arguments
     * @param arg argument
     * @return true if the specified arguments contains a given argument
     */
    private static boolean containsArg (String[] args, String arg){
        for (int i = 0; i < args.length; i++){
            if (args[i].charAt(0) == '-' && args[i].contains(arg))
            	return true;
        }
        return false;
    }
    
    /**
	 * Main method
	 * @author Mikel Elkano Ilintxeta
	 * @param args command line arguments ([-p]: print output, [-v]: verbose)
	 * @version 1.0
	 */
    public static void main(String[] args) {
    	
    	/**
    	 * READ ARGUMENTS
    	 */
    	
    	Configuration conf = new Configuration();
    	String[] otherArgs = null;
    	try{
	    	otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
    	}
    	catch(Exception e){
    		System.err.println("\nINITIALIZATION ERROR:\n\n");
    		System.exit(-1);
    	}

    	String[] programArgs = new String[NUM_PROGRAM_ARGS];
        String[] optionalArgs = null;
        
        if (otherArgs.length < NUM_PROGRAM_ARGS || otherArgs.length > (MAX_OPTIONAL_ARGS + NUM_PROGRAM_ARGS)){
            System.err.println("\nUsage: "+COMMAND_STR+"\n");
            System.exit(2);
        }
        if (otherArgs.length > NUM_PROGRAM_ARGS){
            int numOptionalArgs = otherArgs.length - NUM_PROGRAM_ARGS;
            optionalArgs = new String[numOptionalArgs];
            for (int i = 0; i < numOptionalArgs; i++)
                optionalArgs[i] = otherArgs[i];
            for (int i = numOptionalArgs; i < otherArgs.length; i++)
                programArgs[i - otherArgs.length + NUM_PROGRAM_ARGS] = otherArgs[i];
            // Check optional args
            if (!validOptionalArgs(optionalArgs)){
                System.err.println("\nUsage: "+COMMAND_STR+"\n");
                System.exit(2);
            }
        }
        else
            programArgs = otherArgs;
        
        String hdfsLocation = programArgs[0];
        String paramsPath = programArgs[1];
        String headerPath = programArgs[2];
        String databasePath = programArgs[3];
        String ruleBasePath = programArgs[4];
        String inputPath = programArgs[5];
        String outputPath = programArgs[6];
        
        /**
         * SAVE BASIC PARAMETERS
         */
    	Mediator.setConfiguration(conf);
        Mediator.saveHDFSLocation(hdfsLocation);
        Mediator.saveClassifierDatabasePath(databasePath);
        Mediator.saveClassifierRuleBasePath(ruleBasePath);
        Mediator.saveClassifierInputPath(inputPath);
        Mediator.saveClassifierOutputPath(outputPath);
        
        /**
         * READ CONFIGURATION FILE
         */
        try{
        	Mediator.storeConfigurationParameters(hdfsLocation+"/"+paramsPath);
        }
        catch(Exception e){
        	System.err.println("ERROR READING CONFIGURATION FILE:\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        /**
         * READ HADOOP CONFIGURATION
         */
        try {
        	Mediator.readHadoopConfiguration();
        }
        catch(Exception e){
        	System.err.println("ERROR READING HADOOP CONFIGURATION:\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        /**
         * READ THE ALGORITHM PARAMETERS AND HEADER FILE
         */
        try{
        	Mediator.readClassNumExamplesFromHeaderFile(headerPath);
        	Mediator.readClassifierConfiguration();
        }
        catch(Exception e){
        	System.err.println("ERROR READING CLASSIFIER CONFIGURATION:\n");
        	e.printStackTrace();
        	System.exit(-1);
        }
        
        // Check number of classes
        if (Mediator.getNumClasses() > 127){
        	System.err.println("\nERROR: The maximum number of classes is 127\n");
        	System.err.println(-1);
        }
    	
        /**
         * PREPARE AND RUN MAP REDUCE JOBS
         */
    	try {
    		Job job = Job.getInstance(conf);
            job.setJarByClass(MapReduceLauncher.class);
            job.setMapperClass(ConfusionMatrixMapper.class);
            job.setReducerClass(ConfusionMatrixReducer.class);
            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            job.setOutputKeyClass(ByteWritable.class);
            job.setOutputValueClass(IntArrayWritable.class);
            FileInputFormat.addInputPath(job, new Path(Mediator.getHDFSLocation()+"/"+Mediator.getClassifierInputPath()));
            FileInputFormat.setMaxInputSplitSize(job, Mediator.computeHadoopSplitSize(
        		Mediator.getHDFSLocation()+"/"+Mediator.getClassifierInputPath(), Mediator.getHadoopNumMappers()));
            FileInputFormat.setMinInputSplitSize(job, Mediator.computeHadoopSplitSize(
        		Mediator.getHDFSLocation()+"/"+Mediator.getClassifierInputPath(), Mediator.getHadoopNumMappers()));
            FileOutputFormat.setOutputPath(job, new Path(Mediator.getHDFSLocation()+Mediator.getClassifierTmpOutputPath()));
            job.waitForCompletion(true);
    	}
    	catch(Exception e){
    		System.err.println("\nERROR RUNNING MAPREDUCE JOBS:\n");
    		e.printStackTrace();
    		System.exit(-1);
    	}
    	
    	/**
         * MERGE OUTPUT FILES
         */
    	try {
			mergeConfusionMatrix();
		} catch (Exception e) {
			System.err.println("\nERROR MERGING OUTPUT FILES");
			e.printStackTrace();
		}
    	
    	/**
    	 * SORT AND WRITE CONFUSSION MATRIX
    	 */
    	try {
    		writeConfusionMatrix(sortConfusionMatrix());
    	}
    	catch(Exception e){
    		System.err.println("\nERROR BUILDING CONFUSSION MATRIX\n");
    		e.printStackTrace();
    	}
    	        
    }
    
    /**
     * Merges the output of reducers (confusion matrices)
     * @throws IOException 
     */
    private static void mergeConfusionMatrix() throws IOException {
    	
    	FileSystem fs = FileSystem.get(Mediator.getConfiguration());
    	FileStatus[] status = fs.listStatus(new Path(Mediator.getHDFSLocation()+
    			Mediator.getClassifierTmpOutputPath()));
    	Writer writer = SequenceFile.createWriter(Mediator.getConfiguration(), 
    			Writer.file(new Path(Mediator.getHDFSLocation()+Mediator.getClassifierOutputPath())), 
    			Writer.keyClass(ByteWritable.class), Writer.valueClass(IntArrayWritable.class));
		Reader reader;
		ByteWritable classIndex = new ByteWritable();
        IntArrayWritable classRow = new IntArrayWritable(new int[Mediator.getNumClasses()]);
    	
    	// Read all sequence files from stage 1
    	for (FileStatus fileStatus:status){
    		
    		if (!fileStatus.getPath().getName().contains("_SUCCESS")){
    		
	    		reader = new Reader(Mediator.getConfiguration(), Reader.file(fileStatus.getPath()));

	            while (reader.next(classIndex, classRow))
	            	writer.append(classIndex, classRow);
	            
	            reader.close();
	            
    		}
    		
    	}
    	
    	writer.close();
    	
    	// Remove Stage 1 output
    	fs.delete(new Path(Mediator.getHDFSLocation()+Mediator.getClassifierTmpOutputPath()),true);
    	
    }
    
    /**
     * Returns the list of optional args
     * @return list of optional args
     */
    private static String printOptionalArgs() {
        String output = "";
        for (int i = 0; i < MAX_OPTIONAL_ARGS; i++)
            output += " [-" + VALID_OPTIONAL_ARGS[i] + "]";
        return output + " ";
    }
    
    /**
     * Sorts the confusion matrix (the outputs of reducers are not sorted)
     * @throws IOException 
     */
    private static IntArrayWritable[] sortConfusionMatrix () throws IOException {
    	
    	IntArrayWritable[] confusionMatrix = new IntArrayWritable[Mediator.getNumClasses()];
    	
    	// Read unsorted confusion matrix
    	Reader reader = new Reader(Mediator.getConfiguration(), 
    			Reader.file(new Path(Mediator.getHDFSLocation()+Mediator.getClassifierOutputPath())));
    	ByteWritable classIndex = new ByteWritable();
        IntArrayWritable classRow = new IntArrayWritable(new int[Mediator.getNumClasses()]);
        
        // Sort confusion matrix
        while (reader.next(classIndex, classRow)) {
        	confusionMatrix[classIndex.get()] = new IntArrayWritable(classRow.getData());
        }	
        
        reader.close();
        
        return confusionMatrix;
    	
    }
    
    /**
     * Returns true if the specified optional arguments are valid
     * @param args optional arguments
     * @return true if the specified optional arguments are valid
     */
    private static boolean validOptionalArgs (String[] args){
        boolean validChar;
        // Iterate over args
        for (int i = 0; i < args.length; i++){
            if (args[i].charAt(0)!='-')
            	return false;
            // Iterate over characters (to read combined arguments)
            for (int charPos = 1; charPos < args[i].length(); charPos++){
            	validChar = false;
            	for (int j = 0; j < VALID_OPTIONAL_ARGS.length; j++){
	                if (args[i].charAt(charPos) == VALID_OPTIONAL_ARGS[j]){
	                    validChar = true;
	                    break;
	                }
            	}
            	if (!validChar)
            		return false;
        	}
        }
        return true;
    }
    
    /**
     * Writes the confusion matrix
     * @param confusionMatrix input confusion matrix
     * @throws IOException 
     */
    private static void writeConfusionMatrix (IntArrayWritable[] confusionMatrix) throws IOException{
    	
        Path pt = new Path(Mediator.getHDFSLocation()+Mediator.getClassifierOutputPath());
        FileSystem fs = FileSystem.get(Mediator.getConfiguration());
        fs.delete(pt, false);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt,true)));
        
        // Write table heading
        for (byte i = 0; i < Mediator.getNumClasses(); i++)
        	bw.write("\t"+Mediator.getClassLabel(i));
        bw.write("\n");
        
        // Write matrix content
        for (byte i = 0; i < Mediator.getNumClasses(); i++){
        	bw.write(Mediator.getClassLabel(i));
        	for (byte j = 0; j < Mediator.getNumClasses(); j++)
        		bw.write("\t"+confusionMatrix[i].getData()[j]);
        	bw.write("\n");
        }
        
        bw.close();
        fs.close();
    	
    }

}
