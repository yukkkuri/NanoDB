package Nonbuffer;
import java.io.*;
import java.util.*;

public class MainDB {
	
	public static final int max = 1000000;
	
		public static void main(String[] args) throws Exception {
			
			Scanner input =new Scanner(System.in);
			String[] all = input.nextLine().split(",");
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
		
		public static void copyFile() throws IOException {
			FileInputStream is = new FileInputStream("jointemp.dat");
			FileOutputStream os = new FileOutputStream("jointempcopy.dat");
			byte[] buffer = new byte[4096];
	        int length;
	        while ((length = is.read(buffer)) > 0) {
	            os.write(buffer, 0, length);
	        }
	        is.close();
	        os.close();
		}
		
		
}
