package com.lsu.shim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import com.lsu.objects.Query;
import com.lsu.objects.RegistryEntry;
import com.lsu.test.ClientQueryExampleThread;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

public class Interface_Hector {
	
	/*private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	private static ArrayBlockingQueue<Query> causalQueue;
	public static long latency_read =  2;//All latencies given in ms
	public static long latency_insert =  2;//All latencies given in ms
	public static long latency_update =  2;//All latencies given in ms
	
	public static long latency_SLA =  5;//All latencies given in ms
	public static long curr_deadline =  5;//All latencies given in ms
	
	public static void callMutatorExecute(Mutator<String> mutator){
		mutator.execute();
	}
	
	public static QueryResult<CqlRows<String, String, Long>> callUpdateExecute(CqlQuery<String,String,Long> cqlQuery){
		QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
		return result;
	}
	
	public static QueryResult<HColumn<String, String>> callReadExecute(ColumnQuery<String, String, String> columnQuery){
		QueryResult<HColumn<String, String>> result = columnQuery.execute();
		return result;
	}
	
	public static void insert(Keyspace keyspace, String string, String string2, String string3, String string4, String string5, String string6, String string7, String string8, String string9, String string10, String string11, String string12, int index, long l, long latency_insert2, String cLevel) {
		// TODO Auto-generated method stub
		//HashMap<String,String> map = null;
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.addInsertion(string2, string,
				HFactory.createStringColumn(string4, string3))
				.addInsertion(string2, string, HFactory.createStringColumn(string5, string6))
				.addInsertion(string2, string, HFactory.createStringColumn(string7,string8))
				.addInsertion(string2, string, HFactory.createColumn(string9, string10))
				.addInsertion(string2, string, HFactory.createColumn(string11, string12));
		String operationtype = "insert";
		//System.out.println("***mutator value:-"+mutator);
		scheduleAndQueue(operationtype,mutator,null,null,keyspace,index,l,latency_insert2,cLevel);
		//mutator.execute();
	}

	public static String read(Keyspace keyspace, String string, String string2, String string3, int index, long l, long latency_read2, String cLevel) {
		// TODO Auto-generated method stub
		ColumnQuery<String, String, String> columnQuery =
			    HFactory.createStringColumnQuery(keyspace);
		columnQuery.setColumnFamily(string).setKey(string2).setName(string3);
		
		String operationtype = "read";
		QueryResult<HColumn<String, String>> result = scheduleAndQueue(operationtype,null, columnQuery,null,keyspace,index,l,latency_read, cLevel);
		//QueryResult<HColumn<String, String>> result1 = callReadExecute(columnQuery);
		//QueryResult<HColumn<String, String>> result1 = columnQuery.execute();
		//System.out.println("Executed query: " + result.getQuery());
		return result.toString();
	}

	public static void update(Keyspace keyspace, String string, String string2, String string3,
			String string4, String string5, String string6, int index, long l, long latency_update2, String cLevel) {
		// TODO Auto-generated method stub
		CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, stringSerializer, stringSerializer, longSerializer);
		String cqlStr = "update " + string +" set "+ string5  +" = "+ string6 +", "+ string3 +" = "+ string4 +" WHERE KEY = "+ string2;
		cqlQuery.setQuery(cqlStr);
		String operationtype = "update";
		scheduleAndQueue(operationtype,null, null,cqlQuery,keyspace,index,l,latency_update,cLevel);
		//QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
	}
	
	private static QueryResult<HColumn<String, String>> scheduleAndQueue(String operationtype, Mutator<String> mutator,ColumnQuery<String, String, String> columnQuery, CqlQuery<String,String,Long> cqlQuery, Keyspace keyspace, int index, long l, long latency_insert2, String cLevel)
	{
		Query q = null;
		String key = null;
		UUID idOne = UUID.randomUUID();
		String clId = String.valueOf(idOne) + String.valueOf(System.currentTimeMillis());
		long latencyDep = 0;
		QueryResult<HColumn<String, String>> result = null;
		ArrayList<RegistryEntry> rList = null;
		String CLID = null, STATUS;
		if((DependencyChecker.causalList.contains(index) && ((Query)DependencyChecker.causalList.get(index)).getGuarantee().equalsIgnoreCase("C")) || (DependencyChecker.serializeList.contains(index) && ((Query)DependencyChecker.causalList.get(index)).getGuarantee().equalsIgnoreCase("S")))
		{
			if(DependencyChecker.causalList.contains(index) && ((Query)DependencyChecker.causalList.get(index)).getGuarantee().equalsIgnoreCase("C")){
				while(!((Query)DependencyChecker.causalList.get(index)).getIndex().equalsIgnoreCase(String.valueOf(index)))
				{
					
				}
				//if(((Query)DependencyChecker.causalList.get(index)).getIndex().equalsIgnoreCase(String.valueOf(index)))
				//{
				if(operationtype.equalsIgnoreCase("insert"))
				{
					updateConsistencyLevel(keyspace,cLevel);
					callMutatorExecute(mutator);
				}
				else if(operationtype.equalsIgnoreCase("read")){
					updateConsistencyLevel(keyspace,cLevel);
					result = callReadExecute(columnQuery);
				}
				else if(operationtype.equalsIgnoreCase("update")){
					updateConsistencyLevel(keyspace,cLevel);
					QueryResult<CqlRows<String,String,Long>> result1 = callUpdateExecute(cqlQuery);
				}
				q = causalQueue.remove();
			}
			else{
				if(((Query)DependencyChecker.serializeList.get(index)).getIndex().equalsIgnoreCase(String.valueOf(index))){
					l = System.currentTimeMillis() - l;
					//key = (String) ((HashMap<String,String>)DependencyChecker.serializeList.get(index)).keySet().toArray()[0];
					key = ((Query)DependencyChecker.serializeList.get(index)).getIndex();
					for(int i=Integer.parseInt(key); i<DependencyChecker.serializeList.size();i++)
					{
						latencyDep = latencyDep + ((Query)DependencyChecker.serializeList.get(i)).getLatency();
					}
					curr_deadline = curr_deadline - (System.currentTimeMillis()-ClientQueryExample.tInv);
					writeToRegister(((Query)DependencyChecker.serializeList.get(index)).getKey(),index,clId,l,latencyDep,curr_deadline,"OFF",keyspace);
					rList = readFromRegister(key, keyspace);
					CLID = Scheduler.schedule(rList).split(":")[0];
					STATUS = Scheduler.schedule(rList).split(":")[1];
					while(!CLID.equalsIgnoreCase(ClientQueryExample.CLID) || !STATUS.equalsIgnoreCase("OFF"))
					{
						
					}
					//Scheduler.requestLock(keyspace,key,CLID,index);
					if(operationtype.equalsIgnoreCase("insert")){
						updateConsistencyLevel(keyspace,cLevel);
						callMutatorExecute(mutator);
					}
					else if(operationtype.equalsIgnoreCase("read")){
						updateConsistencyLevel(keyspace,cLevel);
						result = callReadExecute(columnQuery);
					}
					else if(operationtype.equalsIgnoreCase("update")){
						updateConsistencyLevel(keyspace,cLevel);
						QueryResult<CqlRows<String,String,Long>> result1 = callUpdateExecute(cqlQuery);
					}
					
					//Scheduler.releaseLock(keyspace,key,CLID,index);
					if(causalQueue.peek()!=null)
					{
						q = causalQueue.remove();
						//Scheduler.removeregistry(keyspace,key,CLID,index);
					}
				}
				else{
					
				}
			}
		}
		else
		{
			//System.out.println("In eventual guarantee case:"+mutator);
			if(operationtype.equalsIgnoreCase("insert"))
			{
				updateConsistencyLevel(keyspace,cLevel);
				callMutatorExecute(mutator);
			}
			else if(operationtype.equalsIgnoreCase("read")){
				updateConsistencyLevel(keyspace,cLevel);
				result = callReadExecute(columnQuery);
			}
			else if(operationtype.equalsIgnoreCase("update")){
				updateConsistencyLevel(keyspace,cLevel);
				QueryResult<CqlRows<String,String,Long>> result1 = callUpdateExecute(cqlQuery);
			}
			//if(causalQueue.peek()!=null)
				//q = causalQueue.remove();
		}
		return result;
	}
	
	private static ArrayList<RegistryEntry> readFromRegister(String key, Keyspace keyspace) {
		// TODO Auto-generated method stub
		//Map<String, String> resultMap = new HashMap<String, String>();
		RangeSlicesQuery<String, String, String> allRowsQuery = HFactory.createRangeSlicesQuery(keyspace,  StringSerializer.get(),  StringSerializer.get(),  StringSerializer.get());
	    allRowsQuery.setColumnFamily("registry");
	    QueryResult<OrderedRows<String, String, String>> res = allRowsQuery.execute();
        OrderedRows<String, String, String> rows = res.get();
        int rowCnt = 0;
        ArrayList<RegistryEntry> rList = new ArrayList<RegistryEntry>();
        RegistryEntry r = null;
        for (Row<String, String, String> aRow : rows) {
            //if (!resultMap.containsKey(aRow.getKey())) { 
        	if(aRow.getKey().equalsIgnoreCase(key)){
        		r = new RegistryEntry();
        		r.setKey(aRow.getKey());
        		r.setCLID(aRow.getColumnSlice().getColumnByName("CLID").getValue());
        		r.setDEADLINE(aRow.getColumnSlice().getColumnByName("DEADLINE").getValue());
        		r.setINDEX(aRow.getColumnSlice().getColumnByName("INDEX").getValue());
        		r.setLATENCYDEP(aRow.getColumnSlice().getColumnByName("LATENCYDEP").getValue());
        		r.setWAITINGTIME(aRow.getColumnSlice().getColumnByName("WAITINGTIME").getValue());
        		r.setSTATUS(aRow.getColumnSlice().getColumnByName("STATUS").getValue());
        		rList.add(r);
        	}
                //resultMap.put(aRow.getKey(), ++rowCnt);
                //System.out.println(aRow.getKey() + ":" + rowCnt);
            //}
        }
		return rList;
	}
	
	private static void writeToRegister(String key, int index, String clId,
			long l, long latencyDep, long curr_deadline2, String string, Keyspace keyspace) {
		// TODO Auto-generated method stub
		Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
		mutator.addInsertion(key, "registry", HFactory.createStringColumn("CLID", clId))
			    .addInsertion(key, "registry", HFactory.createStringColumn("INDEX", String.valueOf(index)))
			    .addInsertion(key, "registry", HFactory.createStringColumn("WAITINGTIME", String.valueOf(l)))
			    .addInsertion(key, "registry", HFactory.createStringColumn("LATENCYDEP", String.valueOf(latencyDep)))
				.addInsertion(key, "registry", HFactory.createStringColumn("DEADLINE", String.valueOf(curr_deadline2)))
				.addInsertion(key, "registry", HFactory.createStringColumn("STATUS", string));
		callMutatorExecute(mutator);
	}
	
	public static void pushQueues(){
		// TODO Auto-generated method stub
		String key = null;
		if(DependencyChecker.causalList.size()>0)
			causalQueue = new ArrayBlockingQueue<Query>(DependencyChecker.causalList.size());
		HashMap<String,String> map = null;
		if(DependencyChecker.serializeList!=null && DependencyChecker.serializeList.size()>0){
			for(int i=0;i<DependencyChecker.serializeList.size();i++)
			{
				//map = new HashMap<String,String>(); 
				//key = (String) ((HashMap<String,String>)DependencyChecker.serializeList.get(i)).keySet().toArray()[0];
				//map.put(((HashMap<String,String>)DependencyChecker.serializeList.get(i)).get(key), "S");
				causalQueue.add(DependencyChecker.serializeList.get(i));
			}
		}
		else{
			for(int i=0;i<DependencyChecker.causalList.size();i++)
			{
				//map = new HashMap<String,String>(); 
				//map.put(DependencyChecker.causalList.get(i), "C");
				causalQueue.add(DependencyChecker.causalList.get(i));
			}
		}
		
	}
	public static void updateConsistencyLevel(Keyspace keyspace, String cLevel) {
		// TODO Auto-generated method stub
		//Statement st = new SimpleStatement("CONSISTENCY all");
		//st.setConsistencyLevel(ConsistencyLevel.QUORUM);
		//getSession().execute(st) ;
		//CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, stringSerializer, stringSerializer, longSerializer);
		//String cqlStr = "CONSISTENCY all;";
		//cqlQuery.setQuery(cqlStr);
		//QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
		Cluster cluster = Cluster.builder()
				  .addContactPoint("172.31.26.150")
				  .build();
	    Session session = cluster.connect();
	    session.execute(st);
	}
	
	public static Keyspace callInterface(String className) {
		// TODO Auto-generated method stub
		String cLevel= "ALL",condn = "price>20";
		DependencyChecker.getGuarantee(condn,className);
		if(!Scheduler.checkschedulability())
		{
			System.out.println("Not Schedulable under given SLA");
			System.exit(1);
		}
		pushQueues();
		Verifier.chooseConsistencyLevels();  
		//Keyspace keyspace = ClientQueryExample.callClient(cLevel, condn);
		return null;
		//return keyspace;
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String cLevel= "ALL",condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExample.java";
		//DependencyChecker.getGuarantee(condn,className);
		//Keyspace keyspace = ClientQueryExample.callClient(cLevel, condn);
		//ClientQueryExample.RetailStore();
		
		
	}*/

}
