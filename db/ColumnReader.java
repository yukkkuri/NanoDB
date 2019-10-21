package db;

import java.io.*;
import java.util.*;

public class ColumnReader {
	
	int count;
	int nextcount; // for next method
	//private DataInputStream in;
	private String path;
	private Loader target;
	int width;
	int[] column;
	
	public ColumnReader(String s,Loader[] tables) throws Exception {
		this.target = findtable(s,tables);
		this.path = target.output;
		this.width = target.width;
		int c = Integer.parseInt(s.substring(3,s.length()));
		column = new int[target.height];
		makeColumn(c);
		//this.path = target.path.substring(0, target.path.length()-4)+"_c"+s.substring(3,s.length())+".dat";
		//in = new DataInputStream(new FileInputStream(path));
	}
	
	public ColumnReader(int c,int l,int w) throws Exception {
		this.path = "jointempcopy.dat";
		this.target = null;
		column = new int[l];
		this.width = w;
		makeColumn(c);
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
	
	public Map<Integer,Integer> readsome(Map<Character, ArrayList<Integer>> addicons,Map<Character, ArrayList<Integer>> prevjoin) {
		ArrayList<Integer> con=null;
		if(addicons.containsKey(target.name)&&prevjoin.containsKey(target.name)) {
			con = addicons.get(target.name);
			con.retainAll(addicons.get(target.name));
		}
		else if(addicons.containsKey(target.name))
			con = addicons.get(target.name);
		else if(prevjoin.containsKey(target.name))
			con = prevjoin.get(target.name);
		else
			;
		Map<Integer,Integer> res = new HashMap<>();
		if(con==null) 
			for(int i=0;i<column.length;i++)
				res.put(i, column[i]);
		else
			for(int i:con) 
				res.put(i, column[i]);
		return res;
	}
	
	private void makeColumn(int c) throws Exception{
		DataInputStream in = new DataInputStream(new FileInputStream(path));
		int cursor = 0;
    	for(int i=0;i<c;i++)
    		in.readInt();
        while(cursor<column.length) {
        	column[cursor++]=in.readInt();
        	if(cursor<column.length)
        		in.skip(4*(width-1));
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
    		else{
    			if(!m.get(curr).contains(i))
    			m.get(curr).add(i);
    		}
		}
		count+=arraylength;
		return m;
	}
	
	public Map <Integer,List<Integer>> readall(Map<Character, ArrayList<Integer>> addicons,Map<Character, ArrayList<Integer>> prevjoin) {
		ArrayList<Integer> con=null;
		if(addicons.containsKey(target.name)&&prevjoin.containsKey(target.name)) {
			con = addicons.get(target.name);
			con.retainAll(addicons.get(target.name));
		}
		else if(addicons.containsKey(target.name))
			con = addicons.get(target.name);
		else if(prevjoin.containsKey(target.name))
			con = prevjoin.get(target.name);
		else
			;
		Map <Integer,List<Integer>> m = new HashMap<>();
		if(con!=null) {
			for(int i:con) {
				int curr=column[i];
				if(!m.containsKey(curr)) 
	    			m.put(curr, new ArrayList<Integer>(Arrays.asList(i)));
	    		else {
	    			if(!m.get(curr).contains(i))
	    			m.get(curr).add(i);
	    		}
			}
			return m;
		}
		for(int i=0;i<target.height;i++) {
    		int curr=column[i];
    		if(!m.containsKey(curr)) 
    			m.put(curr, new ArrayList<Integer>(Arrays.asList(i)));
    		else
    			{
    			if(!m.get(curr).contains(i))
    			m.get(curr).add(i);
    		}
		}
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