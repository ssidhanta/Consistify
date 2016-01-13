package com.lsu.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.UUID;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.lsu.shim.DependencyChecker;
import com.lsu.shim.Interface;
import com.lsu.shim.InterfaceFixed;
import com.lsu.shim.Verifier;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;

/*
 *  The example Client Application
 */

public class CallClientFixed {
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	

	public static void main(String[] args) {
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExample.java";
		//DependencyChecker.getGuarantee(condn,className);
		//Keyspace keyspace = ClientQueryExample.callClient(cLevel, condn);
		String CLID; 
		long tInv;
		int threadCnt = 16; 
		String cLevel = "ALL";
		long totalTime = System.currentTimeMillis();
		ClientQueryExampleThreadFixed.host = "172.31.36.142";
		ClientQueryExampleThreadFixed.port = "9042";
		if(args.length>0 && args[0]!=null)
			threadCnt = Integer.parseInt(args[0]);
		if(args.length>1 && args[1]!=null && !"".equalsIgnoreCase(args[1].trim()))
			cLevel = args[1].trim();
		if(args.length>2 && args[2]!=null && !"".equalsIgnoreCase(args[2].trim()))
			InterfaceFixed.latency_SLA = Integer.parseInt(args[2].trim());
		if(args.length>3 && args[3]!=null && !"".equalsIgnoreCase(args[3].trim()))
			ClientQueryExampleThreadFixed.replicas = Integer.parseInt(args[3].trim());
		if(args.length>4 && args[4]!=null && !"".equalsIgnoreCase(args[4].trim()))
			ClientQueryExampleThreadFixed.host = args[4].trim();
		if(args.length>5 && args[5]!=null && !"".equalsIgnoreCase(args[5].trim()))
			ClientQueryExampleThreadFixed.port = args[5].trim();
		ClientQueryExampleThreadFixed.threadCnt = threadCnt;
		ClientQueryExampleThreadFixed.cLevel = cLevel;
		ClientQueryExampleThreadFixed clientQueryExampleThreadFixed = null;
		for(int i=0;i<threadCnt;i++){
			 
			tInv = System.currentTimeMillis();
			UUID idOne = UUID.randomUUID();
			CLID = idOne.toString() + String. valueOf(tInv);
			//System.out.println("*****threadCnt||i:="+i);
			 clientQueryExampleThreadFixed = new ClientQueryExampleThreadFixed(CLID, tInv,i);
			 
			Thread thread = new Thread(clientQueryExampleThreadFixed);
			thread.setName(CLID);
			Runtime.getRuntime().addShutdownHook(new HookFixed(clientQueryExampleThreadFixed));
			thread.start();
			//clientQueryExampleThread.shutdown();
		}
		//clientQueryExampleThread.shutdown();
		//String guarantee = DependencyChecker.getGuarantee(condn,className);
		//callClient("All",condn);
		
	}

}

class HookFixed extends Thread{
	private ClientQueryExampleThreadFixed clientQueryExampleThreadFixed;
	public HookFixed(ClientQueryExampleThreadFixed clientQueryExampleThreadFixed) {
		// TODO Auto-generated constructor stub
		this.clientQueryExampleThreadFixed = clientQueryExampleThreadFixed;
	}

	public void run(){
		//clientQueryExampleThread.shutdown();
		//clientQueryExampleThread.stop();
		clientQueryExampleThreadFixed=null;
		//System.out.println("Hook.run()");
	}
}