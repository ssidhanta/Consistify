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

public class CallClient {
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	

	public static void main(String[] args) {
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExample.java";
		//DependencyChecker.getGuarantee(condn,className);
		//Keyspace keyspace = ClientQueryExample.callClient(cLevel, condn);
		String CLID; 
		long tInv;
		int threadCnt = 16; 
		long totalTime = System.currentTimeMillis();
		//ClientQueryExampleThread.threadCnt =  threadCnt;
		ClientQueryExampleThread.host = "172.31.36.142";
		ClientQueryExampleThread.port = "9042";
		if(args.length>0 && args[0]!=null)
			threadCnt = Integer.parseInt(args[0].trim());
		if(args.length>2 && args[2]!=null && !"".equalsIgnoreCase(args[2].trim()))
			Interface.latency_SLA = Integer.parseInt(args[2].trim());
		if(args.length>3 && args[3]!=null && !"".equalsIgnoreCase(args[3].trim()))
			ClientQueryExampleThread.replicas = Integer.parseInt(args[3].trim());
		if(args.length>4 && args[4]!=null && !"".equalsIgnoreCase(args[4].trim()))
			ClientQueryExampleThread.host = args[4].trim();
		if(args.length>5 && args[5]!=null && !"".equalsIgnoreCase(args[5].trim()))
			ClientQueryExampleThread.port = args[5].trim();
		ClientQueryExampleThread.threadCnt = threadCnt;
		ClientQueryExampleThread clientQueryExampleThread = null;
		for(int i=0;i<threadCnt;i++){
			 
			tInv = System.currentTimeMillis();
			UUID idOne = UUID.randomUUID();
			CLID = idOne.toString() + String. valueOf(tInv);
			//System.out.println("*****threadCnt||i:="+i);
			 clientQueryExampleThread = new ClientQueryExampleThread(CLID, tInv,i);
			 
			Thread thread = new Thread(clientQueryExampleThread);
			thread.setName(CLID);
			Runtime.getRuntime().addShutdownHook(new Hook(clientQueryExampleThread));
			thread.start();
			//clientQueryExampleThread.shutdown();
		}
		//clientQueryExampleThread.shutdown();
		//String guarantee = DependencyChecker.getGuarantee(condn,className);
		//callClient("All",condn);
		
	}

}

class Hook extends Thread{
	private ClientQueryExampleThread clientQueryExampleThread;
	public Hook(ClientQueryExampleThread clientQueryExampleThread) {
		// TODO Auto-generated constructor stub
		this.clientQueryExampleThread = clientQueryExampleThread;
	}

	public void run(){
		//clientQueryExampleThread.shutdown();
		//clientQueryExampleThread.stop();
		clientQueryExampleThread=null;
		//System.out.println("Hook.run()");
	}
}