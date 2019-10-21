package Nonbuffer;

import java.io.*;
import java.util.*;

public class join {
	public static Map<Character, ArrayList<Integer>> cons(String[] predicate,Loader[]tables) throws Exception {
		Joinresult result = new Joinresult();
		for(String pre:predicate) {
			int sign = pre.indexOf('=');
			if(sign<0)
				throw new Exception("Invalid join predicate: "+pre);
			String firstpart = pre.substring(0, sign);
			String secondpart = pre.substring(sign+1,pre.length());
			//第一段加入buffer,匹配第二段
			Map <Integer,List<Integer>> buffer = new ColumnReader(firstpart, tables).readall();
			ColumnReader cr = new ColumnReader(secondpart,tables);
			Joinresult curres = null;
			if(firstpart.charAt(0)==secondpart.charAt(0)) 
				curres = new Joinresult(firstpart.charAt(0));
			else
				curres = new Joinresult(firstpart.charAt(0),secondpart.charAt(0)); 
			while(!cr.nextReachEnd()) {
				int curr = cr.readNext();
				if(buffer.containsKey(curr)) {
					List<Integer> indexlist = buffer.get(curr);
					for(int i:indexlist) 
						if(firstpart.charAt(0)==secondpart.charAt(0))
							curres.addindexsingle(i);
						else
							curres.addindexpair(i, cr.nextcount-1);
				}
			}
			result.calc(curres);
		}
		return result.toMap();
	}
}
