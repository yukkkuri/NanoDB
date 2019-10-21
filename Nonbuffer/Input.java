package Nonbuffer;
import java.io.*;
import java.util.*;
public class Input {
	
	Character[]froms;
	String[]sums;
	String[]wheres;
	String[]lastline;
	
	public Input(Scanner input) {
		String[]lines = new String[4];
		input.nextLine();
		for(int i=0;i<4;i++) {
			lines[i]=input.nextLine();
		}
		String[]pre = lines[0].replaceAll(" ", "").split("[(]");
		
		sums = new String[pre.length-1];
		for(int i=1;i<pre.length;i++) {
			sums[i-1]=pre[i].substring(0, pre[i].indexOf(')'));
		}
		pre = lines[1].replaceAll(" ", "").split("[,]");
		
		froms = new Character[pre.length];
		froms[0]=pre[0].charAt(pre[0].length()-1);
		for(int i=1;i<pre.length;i++) 
			froms[i]=pre[i].charAt(0);
		
		pre = lines[2].replaceAll(" ", "").replaceAll("\r","").split("AND");
		
		wheres = new String[pre.length];
		wheres[0]=pre[0].substring(pre[0].indexOf("WHERE")+5, pre[0].length());
		for(int i=1;i<pre.length;i++) {
			wheres[i]=pre[i].substring(0, pre[i].length());
		}
		
		String temp = lines[3].replaceAll(" ", "");
		if(!temp.equals("AND;")) {
			pre = temp.replaceAll(";","").split("AND");
			lastline = new String[pre.length-1];
			for(int i=1;i<pre.length;i++) 
				lastline[i-1]=pre[i];
		}
	}
	
	//for test
	public Input(String command) {
		String[]lines = command.split("\n");
		String[]pre = lines[0].replaceAll(" ", "").split("[(]");
		
		sums = new String[pre.length-1];
		for(int i=1;i<pre.length;i++) {
			sums[i-1]=pre[i].substring(0, pre[i].indexOf(')'));
		}
		pre = lines[1].replaceAll(" ", "").split("[,]");
		
		froms = new Character[pre.length];
		froms[0]=pre[0].charAt(pre[0].length()-1);
		for(int i=1;i<pre.length;i++) 
			froms[i]=pre[i].charAt(0);
		
		pre = lines[2].replaceAll(" ", "").replaceAll("\r","").split("AND");
		
		wheres = new String[pre.length];
		wheres[0]=pre[0].substring(pre[0].indexOf("WHERE")+5, pre[0].length());
		for(int i=1;i<pre.length;i++) {
			wheres[i]=pre[i].substring(0, pre[i].length());
		}
		
		String temp = lines[3].replaceAll(" ", "");
		if(!temp.equals("AND;")) {
			pre = temp.replaceAll(";","").split("AND");
			lastline = new String[pre.length-1];
			for(int i=1;i<pre.length;i++) 
				lastline[i-1]=pre[i];
		}
	}
}
