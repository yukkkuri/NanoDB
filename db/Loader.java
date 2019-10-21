package db;

import java.io.*;
import java.nio.CharBuffer;
import java.nio.IntBuffer;
import java.util.*;
 
public class Loader {
	int count=0;
	int width=0;
	public int height=0;
    String path;
    String output;
    public char name;
    
    public Loader(String path) throws Exception {
    	//long start = System.currentTimeMillis();
    	this.path = path;
    	//this.output = path.substring(0, path.length()-3)+"dat";
    	this.name = path.charAt(path.length()-5);
    	this.output = name+".dat";
    	load();
    	//for(int i=0;i<width;i++)
    		//makeColumn(i);
    	//long stop = System.currentTimeMillis();
        //System.out.println("Loading used"+(stop - start)+"ms");
    }
    
    public int[] reader(int c) throws Exception {
    	DataInputStream in =
                new DataInputStream(
               new FileInputStream(path.substring(0, path.length()-4)+"_c"+c+".dat"));
    	int[] res = new int[height];
    	int cursor = 0;
        while(cursor<height) 
        	res[cursor++]=in.readInt();
        return res;
    }
    
    
//    private void makeColumn(int c) throws Exception{
//    	DataInputStream in =
//                new DataInputStream(
//               new FileInputStream(output));
//    	String currentPath = path.substring(0, path.length()-4)+"_c"+c+".dat";
//        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(currentPath)));
//		int cursor = 0;
//    	for(int i=0;i<c;i++)
//    		in.readInt();
//        while(cursor<height) {
//        	dos.writeInt(in.readInt());
//        	cursor++;
//        	if(cursor<height)
//        		in.skip(4*(width-1));
//        }
//        in.close();
//        dos.close();
//	}


	private void load() throws IOException, InterruptedException {
        FileReader fr = new FileReader(path);
        DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(output)));
       
        //Thread.sleep(5000);
       
        CharBuffer cb1 = CharBuffer.allocate(4096);
        CharBuffer cb2 = CharBuffer.allocate(4096);
        
        while (fr.read(cb1) != -1) {
            cb1.flip();
 
            int lastNumberStart = 0;
            for (int i = 0; i < cb1.length(); i++) {
                if (cb1.charAt(i) == ',' || cb1.charAt(i) == '\n') {
                    int numRead = Integer.parseInt(cb1.toString().substring(lastNumberStart,i));
                    dos.writeInt(numRead);
                    lastNumberStart = i + 1;
                    count++;
                    if(cb1.charAt(i) == '\n') {
                    	width=count;
                    	count=0;
                    	height++;
                    }
                }
            }
           
            cb2.clear();
            cb2.append(cb1, lastNumberStart, cb1.length());
            width=Math.max(count, width);
            CharBuffer tmp = cb2;
            cb2 = cb1;
            cb1 = tmp;
        }
       
        fr.close();
        dos.close();
    }
 
}

