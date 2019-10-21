package db;
import java.io.*;
import java.util.*;

public class Joinresult {
	List<Character> name;
	List<int[]> indexes;
	int counter;
	static int max = MainDB.max;
	boolean tempFile;
//	Map<Character, ArrayList<Integer>> prevjoin;
	//int type; //0:multiresult 1:single 2:normal 
	
	public Joinresult() {
		name = new ArrayList<Character>();
		indexes = new ArrayList<int[]>();
		counter=0;
		tempFile = false;
//		prevjoin = new HashMap<Character, ArrayList<Integer>>();
	}
	
	public Joinresult(char a,char b) {
		name = new ArrayList<Character>();
		indexes = new ArrayList<int[]>();
		name.add(a);
		name.add(b);
		counter=0;
		tempFile = false;
//		prevjoin = new HashMap<Character, ArrayList<Integer>>();
	}
	
	public Joinresult(char a) {
		name = new ArrayList<Character>();
		indexes = new ArrayList<int[]>();
		name.add(a);
		tempFile = false;
		counter=0;
//		prevjoin = new HashMap<Character, ArrayList<Integer>>();
	}
	
	public int getIndex(char c) {
		int index = -1;
		for(int i=0;i<name.size();i++)
			if(name.get(i)==c)
				index = i;
		if(index==-1) 
			System.out.println("notfound");
		return index;
	}
	
	public void addindexpair(int a,int b,char c1, char c2){
		int[]add = {a,b};
		indexes.add(add);
////		if(prevjoin.containsKey(c1)) {
////			if(!prevjoin.get(c1).contains(a))
//			prevjoin.get(c1).add(a);
//		}
//		else
//			prevjoin.put(c1, new ArrayList<Integer>(Arrays.asList(a)));
//		if(prevjoin.containsKey(c2)) {
//			if(!prevjoin.get(c2).contains(b))
//			prevjoin.get(c2).add(b);
//		}
//		else
//			prevjoin.put(c2, new ArrayList<Integer>(Arrays.asList(b)));
	}
	
	public void addindexpair(int a,int b,DataOutputStream dos) throws IOException {
		int[]add = {a,b};
		indexes.add(add);
		if(indexes.size()==max) {
			tempFile = true;
			byte[]towrite = writeindex(2,indexes);
			indexes.clear();
			dos.write(towrite, 0, towrite.length);
			dos.flush();
			counter+=max;
		}
	}
	
	public void addindexsingle(int a,char c){
		int[]add = {a};
		indexes.add(add);
//		if(prevjoin.containsKey(c)) {
//			if(!prevjoin.get(c).contains(a))
//			prevjoin.get(c).add(a);
//		}
//		else
//			prevjoin.put(c, new ArrayList<Integer>(Arrays.asList(a)));
	}
	
	public void addindexsingle(int a,DataOutputStream dos,char c1, char c2) throws IOException {
		int[]add = {a};
		indexes.add(add);
		
		
		if(indexes.size()==max) {
			tempFile = true;
			byte[]towrite = writeindex(2,indexes);
			indexes.clear();
			dos.write(towrite, 0, towrite.length);
			dos.flush();
			counter+=max;
		}
	}
	
	public Map<Character,ArrayList<Integer>> toMap() {
		Map<Character,ArrayList<Integer>> result = new HashMap<>();
		for(int i=0;i<name.size();i++) {
			result.put(name.get(i), new ArrayList<Integer>());
		}
		for(int i=0;i<name.size();i++) {
			ArrayList<Integer> l = result.get(name.get(i));
			for(int[] row:indexes)
				if(!l.contains(row[i]))
					l.add(row[i]); 
		}
		int localcounter=counter;
		int readingcontrol = indexes.size();
		return result;
	}
	
	public static byte[] writeindex(int width,List<int[]> indexes) {
		int b=0;
		byte[] towrite = new byte[4*width*indexes.size()]; 
		for(int[]index:indexes) 
			for(int i=0;i<width;i++) {
				towrite[b++]=(byte)((index[i] >>> 24));
				towrite[b++]=(byte)((index[i] >>> 16));
				towrite[b++]=(byte)((index[i] >>> 8));
				towrite[b++]=(byte)(index[i]);
			}
		return towrite;
	}
	
	
	
	public void calc(Joinresult r2) throws IOException {
//		prevjoin = new HashMap<Character, ArrayList<Integer>>();
		if(r2.name.isEmpty())
			return;
		if(name.isEmpty()){
			counter=r2.counter;
			tempFile = r2.tempFile;
			name = r2.name;
			indexes = r2.indexes;
//			this.prevjoin = r2.prevjoin;
			return;
		}
		MainDB.copyFile("jointemp.dat","jointempcopy.dat");
		int samecol = 0;
		for(char c:r2.name) 
			if(name.contains(c)) 
				samecol++;
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("jointemp.dat")));
		DataInputStream in = new DataInputStream(new FileInputStream("jointempcopy.dat"));
		List<int[]> res = new ArrayList<int[]>();
		if(samecol==0) {

			int localcounter=counter;
			counter = 0;
			for(int[] l:indexes) {
				for(int[] l2:r2.indexes) {
					int[] currentline = new int[l.length+l2.length];
					for(int i=0;i<l.length;i++) {
						currentline[i]=l[i];
//						if(prevjoin.containsKey(name.get(i))) {
//							if(!prevjoin.get(name.get(i)).contains(l[i]))
//							prevjoin.get(name.get(i)).add(l[i]);
//						}
//						else
//							prevjoin.put(name.get(i), new ArrayList<Integer>(Arrays.asList(l[i])));
					}
					for(int i=0;i<l2.length;i++) {
						currentline[i+l.length]=l2[i];
//						if(prevjoin.containsKey(r2.name.get(i))) {
//							if(!prevjoin.get(r2.name.get(i)).contains(l2[i]))
//							prevjoin.get(r2.name.get(i)).add(l2[i]);
//						}
//						else
//							prevjoin.put(r2.name.get(i), new ArrayList<Integer>(Arrays.asList(l2[i])));
					}
					res.add(currentline);
					
					if(res.size()==max) {
						tempFile = true;
						byte[]towrite = writeindex(currentline.length,res);
						res.clear();
						dos.write(towrite, 0, towrite.length);
						dos.flush();
						counter+=max;
					}
				}
			}
			int readingcontrol = indexes.size();
			while(readingcontrol<localcounter&&name.size()>2) {
				indexes.clear();
				readingcontrol+=max;
				for(int ki=0;ki<max;ki++) {
					int[] l = new int[name.size()];
					for(int q=0;q<name.size();q++)
						l[q] = in.readInt();
					for(int[] l2:r2.indexes) {
						int[] currentline = new int[l.length+l2.length];
						for(int i=0;i<l.length;i++) {
							currentline[i]=l[i];
//							if(prevjoin.containsKey(name.get(i))) {
//								if(!prevjoin.get(name.get(i)).contains(l[i]))
//								prevjoin.get(name.get(i)).add(l[i]);
//							}
//							else
//								prevjoin.put(name.get(i), new ArrayList<Integer>(Arrays.asList(l[i])));
						}
						for(int i=0;i<l2.length;i++) {
							currentline[i+l.length]=l2[i];
//							if(prevjoin.containsKey(r2.name.get(i))) {
//								if(!prevjoin.get(r2.name.get(i)).contains(l2[i]))
//								prevjoin.get(r2.name.get(i)).add(l2[i]);
//							}
//							else
//								prevjoin.put(r2.name.get(i), new ArrayList<Integer>(Arrays.asList(l2[i])));
						}
						res.add(currentline);
						if(res.size()==max) {
							tempFile = true;
							byte[]towrite = writeindex(currentline.length,res);
							res.clear();
							dos.write(towrite, 0, towrite.length);
							dos.flush();
							counter+=max;
						}
					}
				}
			}
			name.addAll(r2.name);
		}
			

		else if(samecol==1) {
			int jj=-1;
			char currname ='-';
			for(int i=0;i<r2.name.size();i++) 
				if(name.contains(r2.name.get(i))) { 
					jj = i;
					currname = r2.name.get(i);
				}
			
			int ii=getIndex(currname);
			int append = (jj==0)?1:0;
			char appendname = r2.name.get(append);
			HashMap<Integer,List<Integer>> localhash = new HashMap<>();
			for(int[] l2:r2.indexes) {
				int curr = l2[jj];
				int app = l2[append];
				if(!localhash.containsKey(curr))
					localhash.put(curr, new ArrayList<Integer>(Arrays.asList(app)));
				else
					localhash.get(curr).add(app);
			}
			
			int localcounter=counter;
			counter=0;
			for(int[] l:indexes) {
				if(localhash.containsKey(l[ii])) 
					for(int app:localhash.get(l[ii])) {
						int[] currentline = new int[l.length+1];
						for(int k=0;k<l.length;k++) {
							currentline[k]=l[k];
//							if(prevjoin.containsKey(name.get(k))) {
//								if(!prevjoin.get(name.get(k)).contains(l[k]))
//								prevjoin.get(name.get(k)).add(l[k]);
//							}
//							else
//								prevjoin.put(name.get(k), new ArrayList<Integer>(Arrays.asList(l[k])));
						}
						currentline[l.length] = app;
						res.add(currentline);
//						if(prevjoin.containsKey(appendname)) {
//							if(!prevjoin.get(appendname).contains(app))
//							prevjoin.get(appendname).add(app);
//						}
//						else
//							prevjoin.put(appendname, new ArrayList<Integer>(Arrays.asList(app)));
					if(res.size()==max) {
						tempFile = true;
						byte[]towrite = writeindex(currentline.length,res);
						res.clear();
						dos.write(towrite, 0, towrite.length);
						dos.flush();
						counter+=max;
					}
				}
			}
			int readingcontrol = indexes.size();
			while(readingcontrol<localcounter) {
				indexes.clear();
				readingcontrol+=max;
				for(int i=0;i<max;i++) {
					int[] l = new int[name.size()];
					for(int q=0;q<name.size();q++)
						l[q] = in.readInt();
					if(localhash.containsKey(l[ii])) 
						for(int app:localhash.get(l[ii])) {
							int[] currentline = new int[l.length+1];
							for(int k=0;k<l.length;k++) {
								currentline[k]=l[k];
//								if(prevjoin.containsKey(name.get(k))) {
//									if(!prevjoin.get(name.get(k)).contains(l[k]))
//									prevjoin.get(name.get(k)).add(l[k]);
//								}
//								else
//									prevjoin.put(name.get(k), new ArrayList<Integer>(Arrays.asList(l[k])));
							}
							currentline[l.length] = app;
//							if(prevjoin.containsKey(appendname)) {
//								if(!prevjoin.get(appendname).contains(app))
//								prevjoin.get(appendname).add(app);
//							}
//							else
//								prevjoin.put(appendname, new ArrayList<Integer>(Arrays.asList(app)));
							res.add(currentline);
						if(res.size()==max) {
							tempFile = true;
							byte[]towrite = writeindex(currentline.length,res);
							res.clear();
							dos.write(towrite, 0, towrite.length);
							dos.flush();
							counter+=max;
						}
					}
				}
			}
			name.add(r2.name.get(append));
		}
		else {
			int add1 = getIndex(r2.name.get(0));
			int add2 = getIndex(r2.name.get(1));
			Set<List<Integer>> localhash = new HashSet<>();
			for(int[] l2:r2.indexes) {
				int app1 = l2[0];
				int app2 = l2[1];
				ArrayList<Integer> pair = new ArrayList<>(Arrays.asList(app1,app2));
				localhash.add(pair);
			}
			
			int localcounter=counter;
			counter = 0;
			for(int[] l:indexes) {
				ArrayList<Integer> pair = new ArrayList<>(Arrays.asList(l[add1],l[add2]));
				if(localhash.contains(pair)) { 
					res.add(l);
					for(int k=0;k<l.length;k++) {
//						if(prevjoin.containsKey(name.get(k))) {
//							if(!prevjoin.get(name.get(k)).contains(l[k]))
//								prevjoin.get(name.get(k)).add(l[k]);
//						}
//			
//						else
//							prevjoin.put(name.get(k), new ArrayList<Integer>((Arrays.asList(l[k]))));
					}
				}
					if(res.size()==max) {
						tempFile = true;
						byte[]towrite = writeindex(l.length,res);
						res.clear();
						dos.write(towrite, 0, towrite.length);
						dos.flush();
						counter+=max;
					}
			}
			int readingcontrol = indexes.size();
			
			while(readingcontrol<localcounter&&name.size()>2) {
				indexes.clear();
				readingcontrol+=max;
				for(int i=0;i<max;i++) {
					int[] l = new int[name.size()];
					for(int q=0;q<name.size();q++)
						l[q] = in.readInt();
					ArrayList<Integer> pair = new ArrayList<>(Arrays.asList(l[add1],l[add2]));
					if(localhash.contains(pair)) { 
						res.add(l);
						for(int k=0;k<l.length;k++) {
//							if(prevjoin.containsKey(name.get(k))) {
//								if(!prevjoin.get(name.get(k)).contains(l[k]))
//								prevjoin.get(name.get(k)).add(l[k]);
//							}
//							else
//								prevjoin.put(name.get(k), new ArrayList<Integer>(Arrays.asList(l[k])));
						}
					}
						if(res.size()==max) {
							tempFile = true;
							byte[]towrite = writeindex(l.length,res);
							res.clear();
							dos.write(towrite, 0, towrite.length);
							dos.flush();
							counter+=max;
					}
				}
			}
		}
		in.close();
		dos.close();
		indexes = res;
		counter +=indexes.size();
	}
}
