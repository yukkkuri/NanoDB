package db;

import java.io.*;
import java.util.*;

public class nonjoin {
	
	static int max = MainDB.max;
	Map<Character, ArrayList<Integer>> addicons;
	List<int[]> indexes;
	boolean tempFile;
	int counter;
	int outcounter;
	
	public static Map<Character, ArrayList<Integer>> cons(String[] s,Loader[]tables) throws Exception {
		Map<Character, ArrayList<Integer>> cons = new HashMap<Character, ArrayList<Integer>>();
		for(String pre:s) {
			int signindex = 0;
			while(pre.charAt(signindex)<'<'||pre.charAt(signindex)>'>')
				signindex++;
			char sign = pre.charAt(signindex);
			Map <Integer,List<Integer>> buffer = new ColumnReader(pre.substring(0, signindex), tables).readall();
			int target = Integer.parseInt(pre.substring(signindex+1,pre.length()));
			ArrayList<Integer> index = new ArrayList<>();
			switch (sign){
			case '=':
				if(buffer.containsKey(target))
					index.addAll(buffer.get(target));
				break;
			case '>':
				for(int i:buffer.keySet()) 
					if(i>target)
						index.addAll(buffer.get(i));
				break;
			case '<':
				for(int i:buffer.keySet()) 
					if(i<target)
						index.addAll(buffer.get(i));
				break;
			}
			if(!cons.containsKey(pre.charAt(0)))
				cons.put(pre.charAt(0), index);
			else
				cons.get(pre.charAt(0)).retainAll(index);
		}
		return cons;
	}
	public nonjoin(String[] s,Loader[]tables, Joinresult result, HashSet<Character> joins) throws Exception{
		tempFile = result.tempFile;
		outcounter = result.counter;
		indexes = new ArrayList<int[]>(result.indexes);
		MainDB.copyFile("jointemp.dat","jointempcopy.dat");
		Map<Character, ArrayList<Integer>> addicons = new HashMap<>();
		for(Loader table:tables) {
			if(!joins.contains(table.name)) {
				ArrayList<Integer> init = new ArrayList<>();
				for(int i=0;i<table.height;i++)
					init.add(i);
				addicons.put(table.name,init);
			}
		}
		for(String pre:s) {
			counter = 0;
			int signindex = 0;
			while(pre.charAt(signindex)<'<'||pre.charAt(signindex)>'>')
				signindex++;
			char sign = pre.charAt(signindex);
			Map <Integer,List<Integer>> buffer = new ColumnReader(pre.substring(0, signindex), tables).readall();
			int target = Integer.parseInt(pre.substring(signindex+1,pre.length()));
			ArrayList<Integer> index = new ArrayList<>();
			switch (sign){
			case '=':
				if(buffer.containsKey(target))
					index.addAll(buffer.get(target));
				break;
			case '>':
				for(int i:buffer.keySet()) 
					if(i>target)
						index.addAll(buffer.get(i));
				break;
			case '<':
				for(int i:buffer.keySet()) 
					if(i<target)
						index.addAll(buffer.get(i));
				break;
			}
			if(!joins.contains(pre.charAt(0))) {
				addicons.get(pre.charAt(0)).retainAll(index);
			}
			else {
				int todeleteindex = result.getIndex(pre.charAt(0));
//				int [] todelete = new ColumnReader(todeleteindex,result.counter,result.name.size()).column;
				DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("jointemp.dat")));
				DataInputStream in = new DataInputStream(new FileInputStream("jointempcopy.dat"));
				List<int[]> res = new ArrayList<int[]>();
				for(int i=0;i<indexes.size();i++) {
					int[] l = indexes.get(i);
					if(index.contains(indexes.get(i)[todeleteindex])) 
						res.add(l);
				}
				int readingcontrol = indexes.size();
				indexes = new ArrayList<>(res);
				while(readingcontrol<outcounter) {
					res.clear();
					readingcontrol+=max;
					for(int i=0;i<max;i++) {
						int[] l = new int[result.name.size()];
						for(int q=0;q<result.name.size();q++)
							l[q] = in.readInt();
						if(index.contains(l[todeleteindex])) 
							indexes.add(l);
						if(indexes.size()==max) {
							tempFile = true;
							byte[]towrite = Joinresult.writeindex(result.name.size(),indexes);
							dos.write(towrite, 0, towrite.length);
							dos.flush();
							indexes.clear();
							counter+=max;
						}
					}
				}
				in.close();
				dos.close();
				outcounter = counter+indexes.size();
			}
		}
		counter+= indexes.size();
	}
}
