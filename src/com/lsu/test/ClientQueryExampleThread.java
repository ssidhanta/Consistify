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
import java.util.concurrent.ArrayBlockingQueue;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.lsu.objects.Query;
import com.lsu.objects.Tuple;
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

public class ClientQueryExampleThread implements Runnable {
	private static final int HIGHER_TIMEOUT = 70000;
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private long tInv;
	private int counter;
	public static int threadCnt;
	private volatile boolean done = false;
	public static Session session;
	public static String host = "172.31.36.91";
	public static String port = "9042";
	public static Tuple tuple;
	
	public int getCounter() {
		return counter;
	}

	public void setCounter(int counter) {
		this.counter = counter;
	}

	private Thread client;
	
	private String CLID =  null;
	
	public long gettInv() {
		return tInv;
	}

	public void settInv(long tInv) {
		this.tInv = tInv;
	}
	
	public String getCLID() {
		return CLID;
	}

	public void setCLID(String cLID) {
		CLID = cLID;
	}

	private static Cluster cluster; 
	public static int replicas = 1; 
	
	public  ClientQueryExampleThread(String CLIDparam, long tInvparam, int i){
		this.CLID = CLIDparam;
		this.tInv = tInvparam;
		this.counter = i;
	}
	
	 public static synchronized Session callClient(String cLevel, String condn){
			//Cluster cluster;
			//Session session;
			 PoolingOptions poolingOptions = new PoolingOptions();
			 poolingOptions
			 	.setCoreConnectionsPerHost(HostDistance.LOCAL,  4)
			 	.setMaxConnectionsPerHost( HostDistance.LOCAL, 32768)
			 	//.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768)
			    .setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
			    .setMaxConnectionsPerHost(HostDistance.REMOTE, 32768)
			    .setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, 16384)
			    .setMaxSimultaneousRequestsPerHostThreshold(HostDistance.REMOTE, 32768);
			    //.setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);
			// Connect to the cluster and keyspace "demo"
			 ClientQueryExampleThread.cluster = Cluster.builder()//.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
					 //.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
					 .withProtocolVersion(ProtocolVersion.V3)
					 //.withPoolingOptions(new PoolingOptions()
			           //.setMaxSimultaneousRequestsPerHostThreshold(HostDistance.LOCAL, 16384)
			           //.setMaxSimultaneousRequestsPerHostThreshold(HostDistance.REMOTE, 2048))
					 .withPort(Integer.parseInt(ClientQueryExampleThread.port))
					 .addContactPoint(ClientQueryExampleThread.host)
					 .withPoolingOptions(poolingOptions).build();
			 //ClientQueryExampleThread.cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(HIGHER_TIMEOUT);
			 //ClientQueryExampleThread.cluster.getConfiguration().getSocketOptions().setConnectTimeoutMillis(HIGHER_TIMEOUT);
			Session session = ClientQueryExampleThread.cluster.connect();
			//String kSpace = "CREATE KEYSPACE IF NOT EXISTS consistify WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'eu-central' : 1, 'ap-northeast' : 1, 'us-west-2' : 1 }";
			String kSpace = "CREATE KEYSPACE IF NOT EXISTS consistify WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'us-west-2' : 1 }";
			//ClientQueryExampleThread.session.execute(kSpace);
			//ClientQueryExampleThread.session.execute("CREATE COLUMNFAMILY IF NOT EXISTS consistify.orders (key text, first text, last text, middle text, number text, price text, PRIMARY KEY(key))");
			//System.out.println("***Callclient  cluster session:="+session);
			return session;
		 }
	
	 public void Retail(String CLID, long tInv, int currthreadCount){
		 String cLevel= "ONE",condn = "first>20", className = "/home/ubuntu/Consistify/src/com/lsu/test/ClientQueryExampleThread.java";//unblock for deploy
			int index = 0;
			Interface.curr_deadline = Interface.latency_SLA;
			//tInv = System.currentTimeMillis();
			//UUID idOne = UUID.randomUUID();
			//CLID = idOne.toString() + String.valueOf(tInv);
			Interface intrface = new Interface();
			ClientQueryExampleThread.tuple = intrface.callInterface(condn, className,CLID, tInv,(int)currthreadCount);
			intrface.insert("consistify","orders","first", "John","last","Smith","middle", "Q", "number", "20", "price", "100", "",index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,ClientQueryExampleThread.tuple);
			index++;
			String price=intrface.read("consistify","orders","first", "jsmith",index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,ClientQueryExampleThread.tuple);
			index++;  
			intrface.update("consistify","orders","last","price","first", "jsmith", "" ,index, System.currentTimeMillis(),Interface.latency_update, cLevel,CLID, tInv,ClientQueryExampleThread.tuple);
			index++;
			String new_price=intrface.read("consistify","orders","first","jsmith", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,ClientQueryExampleThread.tuple);
			index++;
			//String new_price=intrface.read("shopping_cart","orders","jsmith","price", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
			//Thread.currentThread().interrupt();
			/*long totalTime = System.currentTimeMillis()/1000 - tInv;
			System.out.println("***txn/s:=="+(index+1)*threadCnt/totalTime);*/
			long totalTime = (System.currentTimeMillis() - tInv)/1000;
			System.out.println("***totalTime:=="+totalTime*1000);
			if(totalTime<=0)
				totalTime=1;
			System.out.println("***txn/s:=="+((index+1)*this.counter)/totalTime);
			//String new_price=intrface.read("shopping_cart","orders","jsmith","price", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
			
			//Thread.currentThread().interrupt();
			/*long totalTime = System.currentTimeMillis()/1000 - tInv;
			System.out.println("***txn/s:=="+(index+1)*threadCnt/totalTime);*/
			if(ClientQueryExampleThread.threadCnt==this.counter)
			{
				 /*DependencyChecker.queryList=null;
		        DependencyChecker.serializeList=null;
		        DependencyChecker.causalList=null;*/
		        if(ClientQueryExampleThread.tuple!=null)
				{
		        	ClientQueryExampleThread.tuple.getSession().close();
					ClientQueryExampleThread.cluster.close();
					ClientQueryExampleThread.tuple=null;
				}
				System.exit(0);
			}
		} 
	 
    public void start() {
        client = new Thread(this);
        client.start();
    }
    
    public void shutdown() {
        done = true;
        /*DependencyChecker.queryList=null;
        DependencyChecker.serializeList=null;
        DependencyChecker.causalList=null;*/
       /* if(ClientQueryExampleThread.session!=null)
		{
        	ClientQueryExampleThread.session.close();
			cluster.close();
			cluster=null;
		}*/
        /*long totalTime = System.currentTimeMillis()/1000 - tInv;
		System.out.println("***txn/s:=="+(index+1)*threadCnt/totalTime);*/
      }
    
    public void stop() {
    	/* DependencyChecker.queryList=null;
         DependencyChecker.serializeList=null;
         DependencyChecker.causalList=null;*/
        done = true;
      }
    
    @Override
	public synchronized void run() {
		// TODO Auto-generated method stub
    	long threadId = Thread.currentThread().getId();
    	//ShoppingCart(this.CLID, this.tInv,this.counter);
    	Retail(this.CLID, this.tInv,this.counter);
	}
    
    /*public void stop() {
        client =  null;  // UNSAFE!
        client.interrupt();
    }
    */
	public static void main(String[] args) {
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExample.java";
		//DependencyChecker.getGuarantee(condn,className);
		//Keyspace keyspace = ClientQueryExample.callClient(cLevel, condn);
		/*String CLID; 
		long tInv;
		tInv = System.currentTimeMillis();
		UUID idOne = UUID.randomUUID();
		CLID = idOne.toString() + String. valueOf(tInv);
		ClientQueryExampleThread clientQueryExampleThread =  new ClientQueryExampleThread(CLID, tInv,1);
		ClientQueryExampleThread.RetailStore(CLID, tInv, 1);
		int threadCnt = 4; 
		if(args.length>0 && args[0]!=null)
			threadCnt = Integer.parseInt(args[0]);
		for(int i=0;i<threadCnt;i++){
			//System.out.println("*****threadCnt||i:="+i);
			//(new Thread(new ClientQueryExampleThread())).start();
		}*/
		//String guarantee = DependencyChecker.getGuarantee(condn,className);
		//callClient("All",condn);
		
	}

}
