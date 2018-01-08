package cs.Lab2.BoroughOldestTree;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.simple.*;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.Iterator;

public class BoroughOldestTreeReducer extends Reducer<Text,Text,Text,Text> {
	private JSONParser parser = new JSONParser();
	
	private Text oldestTree = new Text("Oldest Tree");
	
	private JSONObject yearBoroughOldestTreeObject = new JSONObject();
	private Text yearBoroughOldestTreeText = new Text();
	
    	// Overriding of the reduce function
	// Input : (All Trees, List of TextJSONsWithBoroughAndYear)
	// Output : (Oldest Tree, TextJSONWithOldestTree)
    	@SuppressWarnings("unchecked")
	@Override
    	protected void reduce(final Text cleI, final Iterable<Text> listevalI, final Context context) throws IOException,InterruptedException
    	{
		// Initiate the local variables
		Long oldestYear = new Long(9999);
		String boroughOldest = "";
		String yearBoroughText;
		JSONObject yearBoroughObject;
		Long currentYear;

		// For each tree
		Iterator<Text> iterator = listevalI.iterator();
		while (iterator.hasNext())
		{
			// Recover the JSON string
			yearBoroughText = iterator.next().toString();

				try {
					// Convert the JSON string to a JSON object
					yearBoroughObject = (JSONObject) this.parser.parse(yearBoroughText);

					// Recover the year of the current tree
					currentYear = (Long) yearBoroughObject.get("year");

					// If the current tree is older than the current oldest tree, we update the oldest tree
				if (currentYear < oldestYear)
				{
					oldestYear = currentYear;
					boroughOldest = (String) yearBoroughObject.get("borough");
				}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

		// Put in a JSON object the year and the borugh of the oldes tree
		yearBoroughOldestTreeObject.put("year", oldestYear);
		yearBoroughOldestTreeObject.put("borough",boroughOldest);

		// Convert the JSON object to a string
		yearBoroughOldestTreeText.set(yearBoroughOldestTreeObject.toJSONString());

		// Output
		context.write(oldestTree,yearBoroughOldestTreeText);
    	}

}
