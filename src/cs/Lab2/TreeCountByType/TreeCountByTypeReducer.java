package cs.Lab2.TreeCountByType;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class TreeCountByTypeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	private IntWritable totalTreeCountByType = new IntWritable();
	
    // Overriding of the reduce function
	// Input : (treeType, List of Numbers of Trees)
	// Output : (treeType, treeCountByType)
    @Override
    protected void reduce(final Text cleI, final Iterable<IntWritable> listevalI, final Context context) throws IOException,InterruptedException
    {
    	// Initiate the local variables
        int sum = 0;
        
        // We count the number of trees
        Iterator<IntWritable> iterator = listevalI.iterator();
        while (iterator.hasNext())
        {
        	sum += iterator.next().get();
        }
        
        // Output
        totalTreeCountByType.set(sum);
        context.write(cleI,totalTreeCountByType);
    }

}
