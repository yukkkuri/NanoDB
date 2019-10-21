package db;

import java.io.*;
import java.util.*;



public class join {
	

	
	public static Joinresult cons(Map<Character, ArrayList<Integer>> nonjoin, String[] predicate,Loader[]tables) throws Exception {
		Joinresult result = new Joinresult();
		int j=0;
		for(int i=0;i<predicate.length;i++) {
			int sign = predicate[i].indexOf('=');
			if(sign<0)
				throw new Exception("Invalid join predicate: "+predicate[i]);
			String firstpart = predicate[i].substring(0, sign);
			String secondpart = predicate[i].substring(sign+1,predicate[i].length());
			if(nonjoin.containsKey(firstpart.charAt(0))||nonjoin.containsKey(secondpart.charAt(0))) {
				String temp = predicate[i];
				predicate[i] = predicate[j];
				predicate[j] = temp;
				j++;
			}
		}
		Map<Character, ArrayList<Integer>> prevjoin = new HashMap<Character, ArrayList<Integer>>();
		for(String pre:predicate) {
			int sign = pre.indexOf('=');
			if(sign<0)
				throw new Exception("Invalid join predicate: "+pre);
			String firstpart = pre.substring(0, sign);
			String secondpart = pre.substring(sign+1,pre.length());
			if(!nonjoin.containsKey(firstpart.charAt(0))&&nonjoin.containsKey(secondpart.charAt(0))){
				String temp = secondpart;
				secondpart = firstpart;
				firstpart = temp;
			}
			Map <Integer,List<Integer>> buffer = new ColumnReader(firstpart, tables).readall(nonjoin,prevjoin);
			Map <Integer,Integer> cr =  new ColumnReader(secondpart,tables).readsome(nonjoin,prevjoin);
			Joinresult curres = null;
			if(firstpart.charAt(0)==secondpart.charAt(0)) 
				curres = new Joinresult(firstpart.charAt(0));
			else
				curres = new Joinresult(firstpart.charAt(0),secondpart.charAt(0)); 
//			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("temp.dat")));
			for(int ki:cr.keySet()){
				int curr = cr.get(ki);
				if(buffer.containsKey(curr)) {
					List<Integer> indexlist = buffer.get(curr);
					for(int i:indexlist) {
						if(firstpart.charAt(0)==secondpart.charAt(0))
							curres.addindexsingle(i,firstpart.charAt(0));
//							curres.addindexsingle(i, dos);
						else
							curres.addindexpair(i, ki,firstpart.charAt(0),secondpart.charAt(0));
//							curres.addindexpair(i, cr.nextcount-1,dos);
//						if(firstpart.charAt(0)==secondpart.charAt(0))
//						curres.indexes.add(new int[]{i});
//					else
//					curres.indexes.add(new int[] {i, cr.nextcount-1});
//					if(curres.indexes.size()==MainDB.max) {
//						curres.tempFile = true;
//						byte[]towrite = Joinresult.writeindex(2,curres.indexes);
//						curres.indexes.clear();
//						dos.write(towrite, 0, towrite.length);
//						dos.flush();
//						curres.counter+=MainDB.max;
//					}
					}
				}
			}
//			dos.close();
			curres.counter += curres.indexes.size();
//			if(curres.counter>MainDB.max)
//				MainDB.copyFile("temp.dat", "jointemp.dat");
			result.calc(curres);
			if(result.counter<2000000)
				prevjoin = result.toMap();
		}
		return result;
	}
}