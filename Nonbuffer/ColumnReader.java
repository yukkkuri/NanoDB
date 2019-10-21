package Nonbuffer;

import java.io.*;
import java.util.*;

public class ColumnReader {
	
	int count;
	int nextcount; // for next method
	//private DataInputStream in;
	private String path;
	private Loader target;
	int[] column;
	
	public ColumnReader(String s,Loader[] tables) throws Exception {
		this.target = findtable(s,tables);
		this.path = target.output;
		int c = Integer.parseInt(s.substring(3,s.length()));
		column = new int[target.height];
		makeColumn(c);
		//this.path = target.path.substring(0, target.path.length()-4)+"_c"+s.substring(3,s.length())+".dat";
		//in = new DataInputStream(new FileInputStream(path));
	}
	
	private Loader findtable(String pred,Loader[] tables) throws FileNotFoundException {
		Loader table = null;
		for(Loader l:tables)
			if(l.name == pred.charAt(0)) 
				table = l;
		if(table==null) 
			throw new FileNotFoundException("cannot find table "+pred.charAt(0));
		return table;
	}
	
	private void makeColumn(int c) throws Exception{
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		int cursor = 0;
    	for(int i=0;i<c;i++)
    		in.readInt();
        while(cursor<target.height) {
        	column[cursor++]=in.readInt();
        	if(cursor<target.height)
        		in.skip(4*(target.width-1));
        }
        in.close();
	}
	
	public boolean bufferReachEnd() {
		return count==target.height;
	}
	
	public boolean nextReachEnd() {
		return nextcount==target.height;
	}
	
	public int intAt(int index){
		return column[index];
	}
	
	public int readNext() {
		return column[nextcount++];
	}
	
	public Map <Integer,List<Integer>> readBuffer(int maxlength){
		int arraylength = Math.min(maxlength, target.height-count);
		Map <Integer,List<Integer>> m = new HashMap<>();
		for(int i=0;i<target.height;i++) {
    		int curr=column[i];
    		if(!m.containsKey(curr)) 
    			m.put(curr, new ArrayList<Integer>(Arrays.asList(i)));
    		else
    			m.get(curr).add(i);
		}
		count+=arraylength;
		return m;
	}
	
	public Map <Integer,List<Integer>> readall() {
		Map <Integer,List<Integer>> m = new HashMap<>();
		for(int i=0;i<target.height;i++) {
    		int curr=column[i];
    		if(!m.containsKey(curr)) 
    			m.put(curr, new ArrayList<Integer>(Arrays.asList(i)));
    		else
    			m.get(curr).add(i);
		}
		return m;
	}
}
