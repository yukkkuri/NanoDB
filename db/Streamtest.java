package db;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class Streamtest {
	
	public static void main(String args[]) throws IOException {
		ArrayList<Integer> l1 = new ArrayList<>(Arrays.asList(3,4,5));
		ArrayList<Integer> l2 = new ArrayList<>(Arrays.asList(4,5));
		l2.retainAll(l1);
		String a="a";
		String b="b";
		System.out.println(a);
		System.out.println(b);
		System.out.println(l2);
		DataInputStream in = new DataInputStream(new FileInputStream("jointemp.dat"));
		int[] xx = new int[40];
		for(int i=0;i<xx.length;i++)
			xx[i]=in.readInt();
		int[] out = {98312,12333,4323,50000,-32136};
		int[] out2 = {0,-12333,43023,50000,32136};
		List<int[]> res = new ArrayList<int[]>();
		res.add(out);
		res.add(out2);
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("testout.dat")));
		byte[] towrite = writeindex(5,res);
		dos.write(towrite,0,towrite.length);
		dos.flush();
		dos.write(towrite,0,towrite.length);
		dos.close();
		for(int i=0;i<20;i++)
			System.out.println(in.readInt());
		in.close();
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
}