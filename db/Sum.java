package db;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.*;

public class Sum {
	
	static int max = MainDB.max;
	
	public static String cons(Map<Character, ArrayList<Integer>> nonjoinresult,Joinresult res, String[] predicates, Loader[] tables, HashSet<Character> joins) throws Exception {
		String output = "";
		List<Long> result = new ArrayList<>();
		List<int[]> indexes = res.indexes;
		if(res.counter==0) {
			for(int i=0;i<predicates.length-1;i++)
				output+=',';
			return output;
		}
		for(String pred: predicates) {
			if(joins.contains(pred.charAt(0))) {
			int predindex = res.getIndex(pred.charAt(0));
			long sum = 0;
			ColumnReader reader = new ColumnReader(pred,tables);
			DataInputStream in = new DataInputStream(new FileInputStream("jointemp.dat"));
			for(int[] l:indexes)
				sum+=reader.intAt(l[predindex]);
			int readingcontrol = indexes.size();
			while(readingcontrol<res.counter) {
				readingcontrol+=max;
				for(int i=0;i<max;i++) {
					int[] l = new int[res.name.size()];
					for(int q=0;q<res.name.size();q++)
						l[q] = in.readInt();
						sum+=reader.intAt(l[predindex]);
					}
			}
			result.add(sum);
			}
			else {
				long sum = 0;
				ColumnReader reader = new ColumnReader(pred,tables);
				for(int i:nonjoinresult.get(pred.charAt(0)))
					sum+=reader.intAt(i);
				result.add(sum*res.counter);
			}
		}
		for(Long l:result) 
			output+=l+",";
		output = output.substring(0, output.length()-1);
		return output;
	}
	
	public static String cons(nonjoin nj,Joinresult res, String[] predicates, Loader[] tables, HashSet<Character> joins) throws Exception {
		String output = "";
		List<Long> result = new ArrayList<>();
		List<int[]> indexes = nj.indexes;
		if(nj.counter==0) {
			for(int i=0;i<predicates.length-1;i++)
				output+=',';
			return output;
		}
		for(String pred: predicates) {
			if(joins.contains(pred.charAt(0))) {
			int predindex = res.getIndex(pred.charAt(0));
			long sum = 0;
			ColumnReader reader = new ColumnReader(pred,tables);
			DataInputStream in = new DataInputStream(new FileInputStream("jointemp.dat"));
			for(int[] l:indexes)
				sum+=reader.intAt(l[predindex]);
			int readingcontrol = indexes.size();
			while(readingcontrol<nj.counter) {
				indexes.clear();
				readingcontrol+=max;
				for(int i=0;i<max;i++) {
					int[] l = new int[res.name.size()];
					for(int q=0;q<res.name.size();q++)
						l[q] = in.readInt();
						sum+=reader.intAt(l[predindex]);
					}
			}
			result.add(sum);
			}
			else {
				long sum = 0;
				ColumnReader reader = new ColumnReader(pred,tables);
				for(int i:nj.addicons.get(pred.charAt(0)))
					sum+=reader.intAt(i);
				result.add(sum*nj.counter);
			}
		}
		for(Long l:result) 
			output+=l+",";
		output = output.substring(0, output.length()-1);
		return output;
	}
}

