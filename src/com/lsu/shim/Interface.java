package com.lsu.shim;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import py4j.GatewayServer;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.lsu.objects.Query;
import com.lsu.objects.RegistryEntry;
import com.lsu.objects.Tuple;
import com.lsu.test.ClientQueryExampleThread;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

public class Interface {
	
	private static StringSerializer stringSerializer = StringSerializer.get();
	private static LongSerializer longSerializer = LongSerializer.get();
	//private ArrayBlockingQueue<Query> causalQueue;
	public static long latency_read =  1;//All latencies given in ms
	public static long latency_insert =  1;//All latencies given in ms
	public static long latency_update =  2;//All latencies given in ms
	
	public static long latency_SLA =  15;//All latencies given in ms
	public static long curr_deadline =  15;//All latencies given in ms
	
	
	public void insert(String keyspace, String string, String string2, String string3, String string4, String string5, String string6, String string7, String string8, String string9, String string10, String string11, String string12, int index, long l, long latency_insert2, String cLevel, String CLID, long tInv, Tuple tuple) {
		// TODO Auto-generated method stub
		UUID idOne = UUID.randomUUID();
		//System.out.println("string8 value:---"+string8);
		//String cqlStr = "INSERT INTO " + keyspace + "." + string +"(key,"+ string3 +","+ string5 +","+ string7 +","+ string9 +","+ string11 + ") VALUES ('"+ string2 + "','"+ string4 +"','"+ string6 +"','"+ string8 +"','"+ string10 +"','"+ string12 + "')";
		String cqlStr = "INSERT INTO " + keyspace + "." + string +"("+ string2 + ","+ string4 +","+ string6 +","+ string8 + ") VALUES ('"+ string3 + "','"+ string5 +"','"+ string7 +"','"+ string9 + "')";
		Statement statement = new SimpleStatement(cqlStr);
		String operationtype = "insert";
		//System.out.println("***insert executed:="+cqlStr);
		scheduleAndQueue(operationtype,statement,tuple.getSession(),index,l,latency_insert2,cLevel,CLID,tInv,tuple);
		
	}

	public String read(String keyspace, String string, String string2, String string3, int index, long l, long latency_read2, String cLevel, String CLID, long tInv, Tuple tuple) {
		// TODO Auto-generated method stub
		String cqlStr = "SELECT * from " + keyspace + "." + string +" WHERE " + string2 + " = '" + string3 + "' ALLOW FILTERING", res = null;
		Statement statement = new SimpleStatement(cqlStr);
		String operationtype = "read";
		//System.out.println("***read executed:-"+cqlStr);
		ResultSet results = scheduleAndQueue(operationtype,statement,tuple.getSession(),index,l,latency_read2,cLevel,CLID,tInv,tuple);
		 if(results!=null){
			 for (Row aRow : results) {
				 res = aRow.getString("price");
			 }
		 }
		 
		return res;
		
	}

	public void update(String keyspace, String string, String string2, String string3,
			String string4, String string5, String string6, int index, long l, long latency_update2, String cLevel, String CLID, long tInv, Tuple tuple) {
		// TODO Auto-generated method stub
		String cqlStr = "update " + keyspace + "." + string +" set "+ string2  +" = '"+ string3 +"' WHERE " + string4 + " = '"+ string5 + "'";
		Statement statement = new SimpleStatement(cqlStr);
		String operationtype = "update";
		//System.out.println("***update executed:="+cqlStr);
		//System.out.println("**statement:="+statement.toString());
		scheduleAndQueue(operationtype,statement,tuple.getSession(),index,l,latency_update2,cLevel,CLID,tInv,tuple);
		//QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
		
	}
	
	public ResultSet scheduleAndQueue(String operationtype, Statement statement, Session session, int index, long l, long latency_insert2, String cLevel, String clId, long tInv, Tuple tuple)
	{
		Query q = null;
		String key = null;
		//UUID idOne = UUID.randomUUID();
		String CLID = null; //String.valueOf(idOne) + String.valueOf(System.currentTimeMillis());
		long latencyDep = 0;
		CopyOnWriteArrayList<RegistryEntry> rList = null;
		String STATUS;
		ResultSet result = null;
		Scheduler scheduler = new Scheduler(); 
		
		if((DependencyChecker.causalList.size()>index && ((Query)DependencyChecker.causalList.get(index)).getGuarantee().equalsIgnoreCase("C")) || (DependencyChecker.serializeList.size()>index && ((Query)DependencyChecker.serializeList.get(index)).getGuarantee().equalsIgnoreCase("S")))
		{
			//System.out.println("***in iof constion causal:serialise");
			if(DependencyChecker.causalList.contains(index) && ((Query)DependencyChecker.causalList.get(index)).getGuarantee().equalsIgnoreCase("C")){
				while(!((Query)DependencyChecker.causalList.get(index)).getIndex().equalsIgnoreCase(String.valueOf(index)))
				{
					//System.out.println("***Waiting in causal queue");
				}
				//if(((Query)DependencyChecker.causalList.get(index)).getIndex().equalsIgnoreCase(String.valu             eOf(index)))
				//{
				statement = updateConsistencyLevel(statement,cLevel,index,tuple);
				result = session.execute(statement);//unblock for deploy
				q = tuple.getCausalQueue().remove();
			}
			else{
				//System.out.println("***in else constionL===="+((Query)tuple.getDependencyChecker().getSerializeList().get(index)).getIndex()+": index"+String.valueOf((int)tuple.getDependencyChecker().getQueryList().size()-index-1));
				if(((Query)DependencyChecker.serializeList.get(index)).getIndex().equalsIgnoreCase(String.valueOf((int)DependencyChecker.queryList.size()-1-index))){
					//System.out.println("1111in else constionL===="+((Query)tuple.getDependencyChecker().getSerializeList().get(index)).getIndex());
					l = System.currentTimeMillis() - l;
					//key = (String) ((HashMap<String,String>)DependencyChecker.serializeList.get(index)).keySet().toArray()[0];
					key = ((Query)DependencyChecker.serializeList.get(index)).getKey();
					for(int i=index; i<DependencyChecker.serializeList.size();i++)
					{
						latencyDep = latencyDep + ((Query)DependencyChecker.serializeList.get(i)).getLatency();
					}
					curr_deadline = curr_deadline - (System.currentTimeMillis()-tInv);
					createRegistry(session);
					
					writeToRegister(((Query)DependencyChecker.serializeList.get(index)).getKey(),index,clId,l,latencyDep,curr_deadline,"OFF",session);//unblock for deploy
					//System.out.println("***After write to register");//for thread "+Thread.currentThread().getName());
					//CLID = readFromRegister(key, session);//unblock for deploy
					//System.out.println("****rlist size:="+ rList.size());
					//if(rList.size()>0)
					//if(CLID!=null)
					//{
						//CLID = scheduler.schedule(rList).split(":")[0];
						//STATUS = scheduler.schedule(rList).split(":")[1];
						
						while(readFromRegister(key, session)!=null && readFromRegister(key, session).contains(":") && readFromRegister(key, session).split(":")[1].equalsIgnoreCase("OFF") && !clId.equalsIgnoreCase(readFromRegister(key, session).split(":")[0]))// || !STATUS.equalsIgnoreCase("OFF"))
						{
							//System.out.println("***Client " + clId +"Waiting for the lock to the column "+key);// for thread "+Thread.currentThread().getName());
							//System.out.println("***Waiting for the lock to the column "+key);// for thread "+Thread.currentThread().getName());
						}
						scheduler.requestLock(session,key,clId,index);//unblock for deploy
						//System.out.println("***After requesting the lock to the columnfor the query index"+ index);
						statement = updateConsistencyLevel(statement,cLevel,index,tuple);
						result = session.execute(statement);//unblock for deploy
						//System.out.println("***After executing the query index"+ index);
						scheduler.releaseLock(session,key,clId,index);//unblock for deploy
						if(tuple.getCausalQueue().peek()!=null)
						{
							q = tuple.getCausalQueue().remove();
							scheduler.removeregistry(session,key,clId,index);//unblock for deploy
						}
						//System.out.println("***After release lock and registry cleanup for the query index"+ index);
					//}
				}
				else{
					
				}
			}
		}
		else
		{
			//System.out.println("In eventual guarantee case:"+mutator);
			statement = updateConsistencyLevel(statement,cLevel,index,tuple);
			//System.out.println("***111 Executing eventual consistent queries statement:="+statement);
			//System.out.println("***222 Executing eventual consistent queries tuple session:="+tuple.getSession());
			result = session.execute(statement);//unblock for deploy
			//if(causalQueue.peek()!=null)
				//q = causalQueue.remove();
		}
		return result;
	}
	
	public void scheduleAndQueueTPCC(int index, String key, long l, String clId, long tInv)
	{
		Scheduler scheduler = new Scheduler();
		Session session = null;  
		String cLevel= "ALL",condn = "price>20";
		ClientQueryExampleThread clientQueryExampleThread = new ClientQueryExampleThread(clId, tInv, 1);
		session = clientQueryExampleThread.callClient(cLevel, condn);//unblock for deploy
		Query q = null;
		//UUID idOne = UUID.randomUUID();
		String CLID = null; //String.valueOf(idOne) + String.valueOf(System.currentTimeMillis());
		long latencyDep = 0;
		l = System.currentTimeMillis() - l;
		//key = (String) ((HashMap<String,String>)DependencyChecker.serializeList.get(index)).keySet().toArray()[0];
		
		curr_deadline = curr_deadline - (System.currentTimeMillis()-tInv);
		createRegistry(session);
		
		writeToRegister(key,index,clId,l,latencyDep,curr_deadline,"OFF",session);//unblock for deploy
		System.out.println("***After write to register");//for thread "+Thread.currentThread().getName());
		//CLID = readFromRegister(key, session);//unblock for deploy
		//System.out.println("****rlist size:="+ rList.size());
		//if(rList.size()>0)
		//if(CLID!=null)
		//{
		//CLID = scheduler.schedule(rList).split(":")[0];
		//STATUS = scheduler.schedule(rList).split(":")[1];
			
		while(readFromRegister(key, session)!=null && readFromRegister(key, session).contains(":") && readFromRegister(key, session).split(":")[1].equalsIgnoreCase("OFF") && !clId.equalsIgnoreCase(readFromRegister(key, session).split(":")[0]))// || !STATUS.equalsIgnoreCase("OFF"))
		{
			//System.out.println("***Client " + clId +"Waiting for the lock to the column "+key);// for thread "+Thread.currentThread().getName());
			//System.out.println("***Waiting for the lock to the column "+key);// for thread "+Thread.currentThread().getName());
		}
		scheduler.requestLock(session,key,clId,index);//unblock for deploy
		System.out.println("***After requesting the lock to the columnfor the query index"+ index);
		
	}
	
	public void clearscheduleAndQueueTPCC(int index, String key, long l, String clId, long tInv)
	{
		Scheduler scheduler = new Scheduler();
		Session session = null;  
		String cLevel= "ALL",condn = "price>20";
		ClientQueryExampleThread clientQueryExampleThread = new ClientQueryExampleThread(clId, tInv, 1);
		session = clientQueryExampleThread.callClient(cLevel, condn);//unblock for deploy
		Query q = null;
		//UUID idOne = UUID.randomUUID();
		String CLID = null; //String.valueOf(idOne) + String.valueOf(System.currentTimeMillis());
		long latencyDep = 0;
		CopyOnWriteArrayList<RegistryEntry> rList = null;
		String STATUS;
		l = System.currentTimeMillis() - l;
		//key = (String) ((HashMap<String,String>)DependencyChecker.serializeList.get(index)).keySet().toArray()[0];
		
		curr_deadline = curr_deadline - (System.currentTimeMillis()-tInv);
		
		System.out.println("***After executing the query index"+ index);
		releaseLock(session,key,clId,index);//unblock for deploy
		removeregistry(session,key,clId,index);//unblock for deploy
		System.out.println("***After release lock and registry cleanup for the query index"+ index);
	}
	
	public void requestLock(Session session, String key, String cLID, int index) {
		// TODO Auto-generated method stub
		setRegister(key, index, cLID, session);
		
	}
	
	private void setRegister(String key, int index, String clId, Session session) {
		// TODO Auto-generated method stub
		String cqlStr = "update consistify.registry set STATUS = 'ON' WHERE KEY = '" + key + "' and CLID = '" + clId + "' and indic = '" + index + "'";
		//System.out.println("****cqlStr:="+cqlStr);
		Statement statement = new SimpleStatement(cqlStr);
		statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		ResultSet result = session.execute(statement);
		
	}
	
	public void releaseLock(Session session, String key, String cLID,
			int index) {
		// TODO Auto-generated method stub
		String cqlStr = "update consistify.registry SET STATUS = 'OFF' WHERE KEY = '" + key + "' and CLID = '" + cLID + "' and INDIC = '" + index +"'";
		Statement statement = new SimpleStatement(cqlStr);
		statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		ResultSet result = session.execute(statement);
	}
	
	public void removeregistry(Session session, String key, String cLID,
			int index) {
		// TODO Auto-generated method stub
		String cqlStr = "delete from consistify.registry WHERE KEY =' " + key + "' and CLID = '" + cLID + "' and INDIC = '" + index +"'";
		Statement statement = new SimpleStatement(cqlStr);
		statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		ResultSet result = session.execute(statement);
	}
	
	//private ArrayList<RegistryEntry> readFromRegister(String key, Session session) {
	private String readFromRegister(String key, Session session) {
		// TODO Auto-generated method stub
		//Map<String, String> resultMap = new HashMap<String, String>();
		String CLID = null, STATUS = null; 
		String cqlStr = "SELECT * from consistify.registry WHERE key='" + key+"'";
		Statement statement = new SimpleStatement(cqlStr);
		ResultSet results = session.execute(statement);
		CopyOnWriteArrayList<RegistryEntry> rList = new CopyOnWriteArrayList<RegistryEntry>();
        RegistryEntry r = null;
        Scheduler scheduler = new Scheduler(); 
        for (Row aRow : results) {
            //if (!resultMap.containsKey(aRow.getKey())) { 
        	if(aRow.getString("CREATETIME")!=null && aRow.getString("key").equalsIgnoreCase(key)){
        		r = new RegistryEntry();
        		r.setKey(aRow.getString("key"));
        		//System.out.println("**222*CLID:=="+aRow.getString("CLID"));
        		r.setCLID(aRow.getString("CLID"));
        		r.setDEADLINE(aRow.getString("DEADLINE"));
        		r.setINDEX(aRow.getString("INDIC"));
        		r.setLATENCYDEP(aRow.getString("LATENCYDEP"));
        		r.setWAITINGTIME(aRow.getString("WAITINGTIME"));
        		r.setSTATUS(aRow.getString("STATUS"));
        		r.setCREATETIME(aRow.getString("CREATETIME"));
        		rList.add(r);
        	}
            //resultMap.put(aRow.getKey(), ++rowCnt);
            //System.out.println(aRow.getKey() + ":" + rowCnt);
            //}
        }
        //CLID = scheduler.schedule(rList).split(":")[0];
        CLID = scheduler.schedule(rList);
        //System.out.println("****CLID:="+CLID);
        return CLID;
	}
	
	private void createRegistry(Session session){
		String cqlStr = "CREATE TABLE IF NOT EXISTS consistify.registry (   key text,   CLID text,   DEADLINE text,   INDIC text,   LATENCYDEP text,   WAITINGTIME text, STATUS text, CREATETIME text,  PRIMARY KEY(key,CLID,INDIC));";
		Statement statement = new SimpleStatement(cqlStr);
		//System.out.println("****consistify cqlStr:="+cqlStr);
		statement.setConsistencyLevel(ConsistencyLevel.ALL);
		session.execute(statement);
	}
	
	private void writeToRegister(String key, int index, String clId,
			long l, long latencyDep, long curr_deadline2, String string, Session session) {
		// TODO Auto-generated method stub
		String cqlStr = "INSERT INTO consistify.registry (KEY, CLID, DEADLINE, INDIC, LATENCYDEP,WAITINGTIME,STATUS, CREATETIME) VALUES ('" + key +"','"+ clId +"','" + + curr_deadline2 + "','" +  index + " ','" + latencyDep + " ','" + l + " ','" + string + " ','" + (long)System.currentTimeMillis() + "') if not exists";
		Statement statement = new SimpleStatement(cqlStr);
		
		//System.out.println("****11consistify cqlStr:="+cqlStr);
		statement.setConsistencyLevel(ConsistencyLevel.ALL);
		session.execute(statement);
		//.executeAsync(arg0)
	}
	
	public static synchronized ArrayBlockingQueue<Query> pushQueues(){
		// TODO Auto-generated method stub
		String key = null;
		ArrayBlockingQueue<Query> causalQueue = null;
		if(DependencyChecker.serializeList.size()>0)
			causalQueue = new ArrayBlockingQueue<Query>(DependencyChecker.serializeList.size());
		else if(DependencyChecker.causalList.size()>0)
			causalQueue = new ArrayBlockingQueue<Query>(DependencyChecker.causalList.size());
		//System.out.println("**causalQueue:="+causalQueue.size());
		if(DependencyChecker.serializeList!=null && DependencyChecker.serializeList.size()>0){
			for(int i=0;i<DependencyChecker.serializeList.size();i++)
			{
				//map = new HashMap<String,String>(); 
				//key = (String) ((HashMap<String,String>)DependencyChecker.serializeList.get(i)).keySet().toArray()[0];
				//map.put(((HashMap<String,String>)DependencyChecker.serializeList.get(i)).get(key), "S");
				causalQueue.add(DependencyChecker.serializeList.get(i));
				//System.out.println("***causalQueue:="+causalQueue.size());
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
		return causalQueue;
	}
	
	public Statement updateConsistencyLevel(Statement statement, String cLevel, int index, Tuple  tuple) {
		// TODO Auto-generated method stub
		//Statement st = new SimpleStatement("CONSISTENCY all");
		for(int i=0; i<DependencyChecker.serializeList.size();i++)
		{
			if(i==index)
				cLevel = DependencyChecker.serializeList.get(i).getConsistencyLevel();
		}
		statement = setConsistencyLevel(statement,cLevel);
		return statement;
	}
	
	public Statement setConsistencyLevel(Statement statement, String cLevel) {
		// TODO Auto-generated method stub
		if(cLevel.equalsIgnoreCase("ALL"))
			statement.setConsistencyLevel(ConsistencyLevel.ALL);
		else if(cLevel.equalsIgnoreCase("QUORUM"))
			statement.setConsistencyLevel(ConsistencyLevel.QUORUM);
		else if(cLevel.equalsIgnoreCase("ONE"))
			statement.setConsistencyLevel(ConsistencyLevel.ONE);
		else if(cLevel.equalsIgnoreCase("TWO"))
			statement.setConsistencyLevel(ConsistencyLevel.TWO);
		else if(cLevel.equalsIgnoreCase("ANY"))
			statement.setConsistencyLevel(ConsistencyLevel.ANY);
		return statement;
	}

	public synchronized Tuple callInterface(String condn, String className, String CLID, long tInv, int currthreadCount) {
		Scheduler scheduler = new Scheduler();
		ClientQueryExampleThread clientQueryExampleThread = new ClientQueryExampleThread(CLID, tInv, currthreadCount);
		// TODO Auto-generated method stub
		//Session session = null;  
		//String cLevel= "ALL",condn = "QTY>20";
		String cLevel= "ONE";
		//Tuple tuple = new Tuple();
		
		if(!scheduler.checkschedulability())
		{
			System.out.println("Not Schedulable under given SLA.");// for thread "+Thread.currentThread().getName());
			System.exit(1);
		}
		
		if(ClientQueryExampleThread.tuple==null || ClientQueryExampleThread.tuple.getSession()==null || ClientQueryExampleThread.tuple.getSession().isClosed())
		{
			if(DependencyChecker.queryList==null || DependencyChecker.queryList.size()<=0)
			{
				DependencyChecker.getGuarantee(condn,className, CLID, tInv);
				//System.out.println("***dependencyChecker tSerializeLis:="+dependencyChecker.getSerializeList().size());
				//tuple.seDependencyChecker();
			}
			ClientQueryExampleThread.tuple =  new Tuple();
			Session session = ClientQueryExampleThread.callClient(cLevel, condn);
			ClientQueryExampleThread.tuple.setSession(session);//unblock for deploy
			//System.out.println("***After ClientQueryExampleThread.tuple session:="+ClientQueryExampleThread.tuple.getSession());
			ArrayBlockingQueue<Query> causalQueue = Interface.pushQueues();
			System.out.println("***DependencyChecker.serializeList.size():="+DependencyChecker.serializeList.size());
			/*while(DependencyChecker.serializeList.size()<=1){
					
			}*/
			if(DependencyChecker.serializeList.size()>1)
				Verifier.chooseConsistencyLevels();  
			ClientQueryExampleThread.tuple.setCausalQueue(causalQueue); 
		}	 
		//tuple.setSession(ClientQueryExampleThread.session);
		return ClientQueryExampleThread.tuple;
	}
	
	public static void main(String[] args) {
		Interface intrface = new Interface();
	    // app is now the gateway.entry_point
		GatewayServer server = new GatewayServer(intrface, 9009);
	    server.start();
	  }

}
