package db;
import java.io.*;

import java.util.*;



public class MainDB {
	
	public static final int max = 5000000;
	
		public static void main(String[] args) throws Exception {
			
			Scanner input =new Scanner(System.in);
			String inputt = input.nextLine();
			if(inputt.indexOf("xxs")>-1) {
				String[] all = inputt.split(",");
				Nonbuffer.Loader[] tables = new Nonbuffer.Loader[all.length];
				for(int i=0;i<all.length;i++) {
					tables[i]=new Nonbuffer.Loader(all[i]);
				}
				int k = input.nextInt();
				
					for(int i=0;i<k;i++) {
						Nonbuffer.Query q = new Nonbuffer.Query(input,tables);
						System.out.println(q.getResult());
					}
			}
			else {
			String[] all = inputt.split(",");
			Loader[] tables = new Loader[all.length];
			for(int i=0;i<all.length;i++) {
				tables[i]=new Loader(all[i]);
			}
			int k = input.nextInt();
				for(int i=0;i<k;i++) {
					Query q = new Query(input,tables);
					System.out.println(q.getResult());
				}
			}
		}
		
		public static void copyFile(String s1,String s2) throws IOException {
			FileInputStream is = new FileInputStream(s1);
			FileOutputStream os = new FileOutputStream(s2);
			byte[] buffer = new byte[4096];
	        int length;
	        while ((length = is.read(buffer)) > 0) {
	            os.write(buffer, 0, length);
	        }
	        is.close();
	        os.close();
		}
}
