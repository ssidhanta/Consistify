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

import com.lsu.objects.Tuple;
import com.lsu.shim.DependencyChecker;
import com.lsu.shim.Interface;
import com.lsu.shim.Verifier;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;



public class ClientQueryExample_Hector {
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	public static long tInv;
	public static String CLID =  null;
	
	/*public static void RetailStore(String CLID, long tInv, long currthreadCount){
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExampleThread.java";//block for deploy
		String cLevel= "all",condn = "price>20", className = "/home/ubuntu/Consistify/src/com/lsu/test/ClientQueryExampleThread.java";//unblock for deploy
		int index = 0;
		Interface.curr_deadline = Interface.latency_SLA;
		//tInv = System.currentTimeMillis();
		//UUID idOne = UUID.randomUUID();
		//CLID = idOne.toString() + String.valueOf(tInv);
		Interface intrface = new Interface();
		Tuple tuple = intrface.callInterface(className,CLID, tInv,(int)currthreadCount);
		intrface.insert("consistify","orders","first", "John","last","Smith","middle", "Q", "number", "20", "price", "100", "",index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
			index++;
			String price=intrface.read("consistify","orders","first", "jsmith",index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
			index++;  
			intrface.update("consistify","orders","last","price","first", "jsmith", "" ,index, System.currentTimeMillis(),Interface.latency_update, cLevel,CLID, tInv,tuple);
			index++;
			String new_price=intrface.read("consistify","orders","first","jsmith", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
			index++;
		if(cluster!=null)
		{
			tuple.getSession().close();
			tuple.setSession(null);
			tuple=null;
			cluster.close();
			cluster=null;
		}
		//Thread.currentThread().interrupt();
		
		if(currthreadCount==threadCnt)
			System.exit(1);
	}
	
    public static void StockTrading(String CLID, long tInv, long currthreadCount){
    	 String cLevel= "all",condn = "QTY>20", className = "/home/ubuntu/Consistify/src/com/lsu/test/ClientQueryExampleThread.java";//unblock for deploy
			int index = 0;
			Interface.curr_deadline = Interface.latency_SLA;
			//tInv = System.currentTimeMillis();
			//UUID idOne = UUID.randomUUID();
			//CLID = idOne.toString() + String.valueOf(tInv);
			Interface intrface = new Interface();
			Tuple tuple = intrface.callInterface(condn, className,CLID, tInv,(int)currthreadCount);
		intrface.insert("stock","stock","STOCK_ID","dsds123", "STOCK_NUM","123","STOCK_DESC","sedefe1213", "STOCK_PRICE", "123", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
		index++;
		intrface.insert("stock","orders","ORDER_ID","dsds123", "STOCK_ID","ffewwe2323","STOCK_NUM","123", "STOCK_PRICE", "123", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
		index++;
		intrface.insert("stock","investors","INVESTOR_ID","dsds123", "INVESTOR_NAME","ffewwe yfguygu","DEPOSIT","123", "CREATE_DATE", "02141984", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
		index++;
		String price=intrface.read("stock","orders","ORDER_ID","sfwrfw3454342", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
		index++;
		String price1=intrface.read("stock","investors","INVESTOR_ID","sfwrfw3454342", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
		index++;
		intrface.update("stock","orders","CONFIRMATION","YES","ORDER_ID", "guui1555", "", index, System.currentTimeMillis(),Interface.latency_update, cLevel,CLID, tInv,tuple);
		index++;
		intrface.insert("stock","order_history","ORDER_ID","dsds123", "STOCK_ID","ffewwe2323","INVESTOR_ID","sfs323", "CREATE_DATE", "02141984", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
		index++;
		//String new_price=intrface.read("shopping_cart","orders","jsmith","price", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
		if(cluster!=null)
		{
			tuple.getSession().close();
			tuple.setSession(null);
			tuple=null;
			cluster.close();
			cluster=null;
		}
		//Thread.currentThread().interrupt();
		
		if(currthreadCount==threadCnt)
			System.exit(1);	
	}
    public static void ShoppingCart(String CLID, long tInv, long currthreadCount){
    	//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExampleThread.java";//block for deploy
    			String cLevel= "all",condn = "QTY>20", className = "/home/ubuntu/Consistify/src/com/lsu/test/ClientQueryExampleThread.java";//unblock for deploy
    			int index = 0;
    			Interface.curr_deadline = Interface.latency_SLA;
    			//tInv = System.currentTimeMillis();
    			//UUID idOne = UUID.randomUUID();
    			//CLID = idOne.toString() + String.valueOf(tInv);
    			Interface intrface = new Interface();
    			Tuple tuple = intrface.callInterface(condn, className,CLID, tInv,(int)currthreadCount);
    			intrface.insert("shopping_cart","catalog","ITEM_ID","dsds123", "ITEM_NAME","ffewwe2323","ITEM_DESC","123", "STATUS", "INACTIVE", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
    			index++;
    			String price=intrface.read("shopping_cart","catalog","STATUS","ACTIVE", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
    			index++;
    			intrface.insert("shopping_cart","cart","ITEM_ID","dsds123", "ITEM_DESC","ffewwe2323","QTY","123", "CREATE_DATE", "02141984", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
    			index++;
    			intrface.insert("shopping_cart","orders","ORDER_ID","dsds123", "ITEM_ID","ffewwe2323","QTY","123", "CART_ID", "swqewq1223", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
    			index++;
    			intrface.insert("shopping_cart","invoice","INVOICE_ID","dsds123", "ORDER_ID","ffewwe2323","AMT","123", "PAID_STATUS", "NO", "", "", "", index, System.currentTimeMillis(),Interface.latency_insert,cLevel,CLID, tInv,tuple);
    			index++;
    			intrface.update("shopping_cart","invoice","PAID_STATUS","PAID","INVOICE_ID", "guui1555", "", index, System.currentTimeMillis(),Interface.latency_update, cLevel,CLID, tInv,tuple);
    			index++;
    			long totalTime = (System.currentTimeMillis() - tInv)/1000;
    			System.out.println("***txn/s:=="+((index+1)*threadCnt)/totalTime);
    			//String new_price=intrface.read("shopping_cart","orders","jsmith","price", index, System.currentTimeMillis(),Interface.latency_read,cLevel,CLID, tInv,tuple);
    			if(cluster!=null)
    			{
    				tuple.getSession().close();
    				tuple.setSession(null);
    				tuple=null; 
    				cluster.close();
    				cluster=null;
    			}
    			//Thread.currentThread().interrupt();
    			long totalTime = System.currentTimeMillis()/1000 - tInv;
    			System.out.println("***txn/s:=="+(index+1)*threadCnt/totalTime);
    			if(currthreadCount==threadCnt)
    			{
    				DependencyChecker.queryList=null;
    		         DependencyChecker.serializeList=null;
    		         DependencyChecker.causalList=null;
    				System.exit(0);
    			}
	}*/
	public static void callClient_old(String cLevel, String condn){
		
		try {
        	
        	/*CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("173.253.225.182");
        	cassandraHostConfigurator.setAutoDiscoverHosts(true);*/
        	Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "172.31.26.150");
			KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("consistify");
			
			KeyspaceDefinition newKeyspace = null;
			Keyspace keyspace = null;
			// If keyspace does not exist, the CFs don't exist either. => create them.
			if (keyspaceDef == null) {
				ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("consistify",
                        "MyColumnFamily",
                        ComparatorType.BYTESTYPE);

				newKeyspace = HFactory.createKeyspaceDefinition("consistify",
				                      ThriftKsDef.DEF_STRATEGY_CLASS,
				                      5,
				                      Arrays.asList(cfDef));
				//Add the schema to the cluster.
				//"true" as the second param means that Hector will block until all nodes see the change.
				cluster.addKeyspace(newKeyspace, true);
				//Create a customized Consistency Level
				ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
				Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
				// Define CL.ONE for ColumnFamily "MyColumnFamily"
		
				//clmap.put("MyColumnFamily", HConsistencyLevel.ONE);
				clmap.put("MyColumnFamily", HConsistencyLevel.valueOf(cLevel));
		
				// In this we use CL.ONE for read and writes. But you can use different CLs if needed.
				configurableConsistencyLevel.setReadCfConsistencyLevels(clmap);
				configurableConsistencyLevel.setWriteCfConsistencyLevels(clmap);
				// Then let the keyspace know
				keyspace = HFactory.createKeyspace("consistify", cluster,configurableConsistencyLevel);
			}
			
			Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
			mutator.addInsertion("jsmith", "MyColumnFamily",
					HFactory.createStringColumn("first", "John"))
					.addInsertion("jsmith", "MyColumnFamily", HFactory.createStringColumn("last", "Smith"))
					.addInsertion("jsmith", "MyColumnFamily", HFactory.createStringColumn("middle", "Q"))
					.addInsertion("jsmith", "MyColumnFamily", HFactory.createStringColumn("number", "20"))
					.addInsertion("jsmith", "MyColumnFamily", HFactory.createStringColumn("price", "100"));
			mutator.execute();
			
			ColumnQuery<String, String, String> columnQuery =
				    HFactory.createStringColumnQuery(keyspace);
			columnQuery.setColumnFamily("MyColumnFamily").setKey("jsmith").setName("number");
			QueryResult<HColumn<String, String>> result1 = columnQuery.execute();
			System.out.println("Executed query: " + result1.getQuery());
			
			CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, stringSerializer, stringSerializer, longSerializer);
			cqlQuery.setQuery("update MyColumnFamily set 'number' = '50', 'price' = '40' WHERE KEY = 'jsmith''");
			QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
			
			ColumnQuery<String, String, String> columnQuery1 =
				    HFactory.createStringColumnQuery(keyspace);
			columnQuery.setColumnFamily("MyColumnFamily").setKey("jsmith").setName("number");
			QueryResult<HColumn<String, String>> result2 = columnQuery.execute();
			System.out.println("Executed query: " + result1.getQuery());
			
			//assertEquals(2, result.get().getAsCount());
			// given Keyspace ko and StringSerializer se
			/*SliceQuery<String, String> q = HFactory.createSliceQuery(ko, se, se, se);
			q.setColumnFamily(cf)
			.setKey("jsmith")
			.setColumnNames("first", "last", "middle");
			Result<ColumnSlice<String, String>> r = q.execute();*/
			
					
        } /*catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/ catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	 public static Keyspace callClient(String cLevel, String condn){
		 Keyspace keyspace = null;
			try {
	        	
	        	/*CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator("173.253.225.182");
	        	cassandraHostConfigurator.setAutoDiscoverHosts(true);*/
	        	Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "172.31.26.150");
				KeyspaceDefinition keyspaceDef = cluster.describeKeyspace("consistify");
				
				//System.out.println("***existing keyspaceDef describe:"+keyspace);
				//KeyspaceDefinition newKeyspace = null;
				// If keyspace does not exist, the CFs don't exist either. => create them.
				if (keyspaceDef == null) {
					//System.out.println("***Within Keyspacedef===null");
					ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition("consistify",
	                        "orders",
	                        ComparatorType.UTF8TYPE);
					
					//cfDef.setName("StandardLong1");
					cfDef.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
					
					BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
					columnDefinition.setName(StringSerializer.get().toByteBuffer("price"));    
					columnDefinition.setValidationClass(ComparatorType.INTEGERTYPE.getClassName());
					cfDef.addColumnDefinition(columnDefinition);
					
					columnDefinition = new BasicColumnDefinition();
					columnDefinition.setName(StringSerializer.get().toByteBuffer("number"));    
					columnDefinition.setValidationClass(ComparatorType.INTEGERTYPE.getClassName());
					cfDef.addColumnDefinition(columnDefinition);
					
					//cfDef.setComparatorType(ComparatorType.UTF8TYPE.getClassName());
					keyspaceDef = HFactory.createKeyspaceDefinition("consistify",
					                      ThriftKsDef.DEF_STRATEGY_CLASS,
					                      3,
					                      Arrays.asList(cfDef));
					//Add the schema to the cluster.
					//"true" as the second param means that Hector will block until all nodes see the change.
					String retAddKeyspace = cluster.addKeyspace(keyspaceDef, true);
					//System.out.println("***value of retAddKeyspace"+retAddKeyspace);
					//Create a customized Consistency Level
					/*ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
					Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
					// Define CL.ONE for ColumnFamily "MyColumnFamily"
			
					//clmap.put("MyColumnFamily", HConsistencyLevel.ONE);
					clmap.put("orders", HConsistencyLevel.valueOf(cLevel));
			
					// In this we use CL.ONE for read and writes. But you can use different CLs if needed.
					configurableConsistencyLevel.setReadCfConsistencyLevels(clmap);
					configurableConsistencyLevel.setWriteCfConsistencyLevels(clmap);
					// Then let the keyspace know
					keyspace = HFactory.createKeyspace("consistify", cluster,configurableConsistencyLevel);
					//System.out.println("***created new Keyspace:"+keyspace);
*/					
				}				
				
				ConfigurableConsistencyLevel configurableConsistencyLevel = new ConfigurableConsistencyLevel();
				Map<String, HConsistencyLevel> clmap = new HashMap<String, HConsistencyLevel>();
				// Define CL.ONE for ColumnFamily "MyColumnFamily"
		
				//clmap.put("MyColumnFamily", HConsistencyLevel.ONE);
				clmap.put("orders", HConsistencyLevel.valueOf(cLevel));
		
				// In this we use CL.ONE for read and writes. But you can use different CLs if needed.
				configurableConsistencyLevel.setReadCfConsistencyLevels(clmap);
				configurableConsistencyLevel.setWriteCfConsistencyLevels(clmap);
				// Then let the keyspace know
				keyspace = HFactory.createKeyspace("consistify", cluster,configurableConsistencyLevel);		
	        } /*catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}*/ catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return keyspace;

		}
	
	/*public static void RetailStore(){
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExample.java";
		String cLevel= "all",condn = "price>20", className = "/home/ubuntu/Consistify/src/com/lsu/test/ClientQueryExample.java";
		int index = 0;
		Keyspace keyspace = Interface.callInterface(className);
		Interface.curr_deadline = Interface.latency_SLA;
		tInv = System.currentTimeMillis();
		UUID idOne = UUID.randomUUID();
		CLID = idOne.toString() + String.valueOf(tInv);
		Interface.insert(keyspace,"orders","jsmith","first", "John","last","Smith","middle", "Q", "number", "20", "price", "100", index, System.currentTimeMillis(),Interface.latency_insert,cLevel);
		index++;
		String price=Interface.read(keyspace,"orders","jsmith","price", index, System.currentTimeMillis(),Interface.latency_read,cLevel);
		index++;
		
		Interface.update(keyspace,"orders","jsmith","price","80", "number", "60", index, System.currentTimeMillis(),Interface.latency_update, cLevel);
		index++;
		String new_price=Interface.read(keyspace,"orders","jsmith","price", index, System.currentTimeMillis(),Interface.latency_read,cLevel);
	}*/
	
    public static void StockTrading(){
		
	}
    
    public static void ShoppingCart(){
		
	}
    
	public static void main(String[] args) {
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExample.java";
		//DependencyChecker.getGuarantee(condn,className);
		//Keyspace keyspace = ClientQueryExample.callClient(cLevel, condn);
		//ClientQueryExample_Hector.RetailStore();
		//String guarantee = DependencyChecker.getGuarantee(condn,className);
		//callClient("All",condn);
		
	}

}
