package Nonbuffer;

import java.io.*;
import java.util.*;

public class Test {
	
	static Loader[] tables;

	public static void main(String[] args) throws Exception {
//		Formal use:
		Scanner input =new Scanner(System.in);
//		String[] all = input.nextLine().split(",");
//		Loader[] tables = new Loader[all.length];
//		int time = input.nextInt();
//		for(int i=0;i<all.length;i++) 
//			tables[i]=new Loader(all[i]);
		String root = "E:/cosidb/PA3/data/data/xxxs/";
		File f = new File(root);
		String[] all = f.list((file, name) -> name.endsWith(".csv"));
		
		tables = new Loader[all.length];
		for(int i=0;i<all.length;i++) 
			tables[i]=new Loader(root+all[i]);
		int k = input.nextInt();
		long start = System.currentTimeMillis();
		for(int i=0;i<k;i++) {
				Query q = new Query(input,tables);
				System.out.println(q.getResult());
		}
		long end = System.currentTimeMillis();
		System.out.println(end-start+"ms");
	}
}
