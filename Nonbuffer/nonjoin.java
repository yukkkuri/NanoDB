package Nonbuffer;

import java.util.*;

public class nonjoin {
	
	
	public static Map<Character, ArrayList<Integer>> cons(String[] s,Loader[]tables,Map<Character, ArrayList<Integer>> result, HashSet<Character> joins) throws Exception {
		for(Loader table:tables) {
			if(!joins.contains(table.name)) {
				ArrayList<Integer> init = new ArrayList<>();
				for(int i=0;i<table.height;i++)
					init.add(i);
				result.put(table.name,init);
			}
		}
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
			if(!joins.contains(pre.charAt(0))) {
				result.get(pre.charAt(0)).retainAll(index);
			}
			else {
			List<Integer> todelete = result.get(pre.charAt(0));
			for(int i=0;i<todelete.size();i++) 
				if(!index.contains(todelete.get(i)))
					for(char delete:result.keySet()) 
						result.get(delete).set(i, -1);
			}
		}
		for(char delete:result.keySet()) 
			result.get(delete).removeAll(new ArrayList<Integer>(Arrays.asList(-1)));
		return result;
	}
}

