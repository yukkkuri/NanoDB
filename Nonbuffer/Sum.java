package Nonbuffer;

import java.io.FileNotFoundException;
import java.util.*;

public class Sum {
	public static String cons(Map<Character,ArrayList<Integer>> res , String[] predicates, Loader[] tables, HashSet<Character> joins) throws Exception {
		String output = "";
		List<Long> result = new ArrayList<>();
		char name = predicates[0].charAt(0);
		int joinlength =0;
		boolean allEmpty = true;
		for(char c:joins) {
			joinlength = res.get(c).size();
			if(!res.get(c).isEmpty())
				allEmpty = false;
		}
		if(allEmpty) {
			for(int i=0;i<predicates.length-1;i++)
				output+=',';
			return output;
		}
		for(String pred: predicates) {
			if(joins.contains(pred.charAt(0))) {
			long sum = 0;
			ColumnReader reader = new ColumnReader(pred,tables);
			for(int i:res.get(pred.charAt(0)))
				sum+=reader.intAt(i);
			result.add(sum);
			}
			else {
				long sum = 0;
				ColumnReader reader = new ColumnReader(pred,tables);
				for(int i:res.get(pred.charAt(0)))
					sum+=reader.intAt(i);
				result.add(sum*joinlength);
			}
		}
		for(Long l:result) 
			output+=l+",";
		output = output.substring(0, output.length()-1);
		return output;
	}
}
