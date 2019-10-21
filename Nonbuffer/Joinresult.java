package Nonbuffer;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class Joinresult {
	List<Character> name;
	List<int[]> indexes;
	boolean tempFile;
	int length;
	//int type; //0:multiresult 1:single 2:normal 
	
	public Joinresult() {
		name = new ArrayList<Character>();
		indexes = new ArrayList<int[]>();
		tempFile = false;
		length = 0;
	}
	
	public Joinresult(char a,char b) {
		name = new ArrayList<Character>();
		indexes = new ArrayList<int[]>();
		name.add(a);
		name.add(b);
		length = 0;
		tempFile = false;
	}
	
	public Joinresult(char a) {
		name = new ArrayList<Character>();
		indexes = new ArrayList<int[]>();
		name.add(a);
		tempFile = false;
		length = 0;
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
	
	public void addindexpair(int a,int b) {
		int[]add = {a,b};
		indexes.add(add);
	}
	
	public void addindexsingle(int a) {
		int[]add = {a};
		indexes.add(add);
	}
	
	public Map<Character,ArrayList<Integer>> toMap() {
		Map<Character,ArrayList<Integer>> result = new HashMap<>();
		for(int i=0;i<name.size();i++) {
			result.put(name.get(i), new ArrayList<Integer>());
		}
		for(int i=0;i<name.size();i++) {
			ArrayList<Integer> l = result.get(name.get(i));
			for(int[] row:indexes)
				l.add(row[i]);
		}
		return result;
	}
	
	
	
	public void calc(Joinresult r2) throws IOException {
		if(r2.name.isEmpty())
			return;
		if(name.isEmpty()) {
			this.indexes = r2.indexes;
			this.name = r2.name;
			this.tempFile = r2.tempFile;
			this.length = r2.length;
			return;
		}
		int samecol = 0;
		for(char c:r2.name) 
			if(name.contains(c)) 
				samecol++;
		
		if(samecol==0) {
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("jointemp.dat")));
			DataInputStream in = new DataInputStream(new FileInputStream("jointempcopy.dat"));
			List<int[]> res = new ArrayList<int[]>();
			
			for(int[] l:indexes) 
				for(int[] l2:r2.indexes) { 
					int[] currentline = new int[l.length+l2.length];
					for(int i=0;i<l.length;i++)
						currentline[i]=l[i];
					for(int i=0;i<l2.length;i++)
						currentline[i+l.length]=l2[i];
					res.add(currentline);
					if(res.size()==MainDB.max) {
						tempFile = true;
						byte[] towrite = MainDB.writeindex(currentline.length, res);
						res.clear();
						dos.write(towrite, 0, towrite.length);
						dos.flush();
					}
				}
			name.addAll(r2.name);
			indexes = res;
		}
		else if(samecol==1) {
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("jointemp.dat")));
			DataInputStream in = new DataInputStream(new FileInputStream("jointempcopy.dat"));
			List<int[]> res = new ArrayList<int[]>();
			int jj=-1;
			char currname ='-';
			for(int i=0;i<r2.name.size();i++) 
				if(name.contains(r2.name.get(i))) { 
					jj = i;
					currname = r2.name.get(i);
				}
			int ii=getIndex(currname);
			int append = (jj==0)?1:0;
			HashMap<Integer,List<Integer>> localhash = new HashMap<>();
			for(int[] l2:r2.indexes) {
				int curr = l2[jj];
				int app = l2[append];
				if(!localhash.containsKey(curr))
					localhash.put(curr, new ArrayList<Integer>(Arrays.asList(app)));
				else
					localhash.get(curr).add(app);
			}
			for(int[] l:indexes) 
				//如果tempFile=true,先读res，再把tempFile内容分批加入res.
				if(localhash.containsKey(l[ii])) 
					for(int app:localhash.get(l[ii])) {
						int[] currentline = new int[l.length+1];
						for(int k=0;k<l.length;k++)
							currentline[k]=l[k];
						currentline[l.length] = app;
						res.add(currentline);
						if(res.size()==MainDB.max) {
							tempFile = true;
							byte[] towrite = MainDB.writeindex(currentline.length, res);
							res.clear();
							dos.write(towrite, 0, towrite.length);
							dos.flush();
						}
					}
			name.add(r2.name.get(append));
			indexes = res;
		}
		else {
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("jointemp.dat")));
			DataInputStream in = new DataInputStream(new FileInputStream("jointempcopy.dat"));
			List<int[]> res = new ArrayList<int[]>();
			int add1 = getIndex(r2.name.get(0));
			int add2 = getIndex(r2.name.get(1));
			Set<List<Integer>> localhash = new HashSet<>();
			for(int[] l2:r2.indexes) {
				int app1 = l2[0];
				int app2 = l2[1];
				ArrayList<Integer> pair = new ArrayList<>(Arrays.asList(app1,app2));
				localhash.add(pair);
			}
			for(int[] l:indexes) { 
				ArrayList<Integer> pair = new ArrayList<>(Arrays.asList(l[add1],l[add2]));
				if(localhash.contains(pair)) 
					res.add(l);
				if(res.size()==MainDB.max) {
					tempFile = true;
					byte[] towrite = MainDB.writeindex(l.length, res);
					res.clear();
					dos.write(towrite, 0, towrite.length);
					dos.flush();
				}
			}
			indexes = res;
		}
	}
}
