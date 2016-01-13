package com.lsu.shim;

import java.util.ArrayList;
import java.util.HashMap;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

import com.lsu.objects.RegistryEntry;

public class Scheduler_hector {

	/*public static boolean checkschedulability() {
		// TODO Auto-generated method stub
		long latency= 0;
		for(int i=0; i<DependencyChecker.causalList.size();i++)
		{
			latency = latency + DependencyChecker.causalList.get(i).getLatency();
		}
		for(int i=0; i<DependencyChecker.serializeList.size();i++)
		{
			latency = latency + DependencyChecker.serializeList.get(i).getLatency();
		}
		if(latency<=Interface.latency_SLA)
			return true;
		else
			return false;
	}
	
	public static String schedule(ArrayList<RegistryEntry> rList) {
		// TODO Auto-generated method stub
		String tempRet=null;
		long min =0;
		RegistryEntry r = null;
		for(int i=0;i<rList.size();i++)
		{ 
			r = rList.get(i);
			if(min>(Long.parseLong(r.getDEADLINE())-Long.parseLong(r.getWAITINGTIME())-Long.parseLong(r.getLATENCYDEP())))
			{
				   	min = Long.parseLong(r.getDEADLINE())-Long.parseLong(r.getWAITINGTIME())-Long.parseLong(r.getLATENCYDEP());
				   	tempRet = r.getCLID() + ":" + r.getSTATUS();
			}	
		}
		return tempRet;
	}
	
	private static void setRegister(String key, int index, String clId, Keyspace keyspace) {
		// TODO Auto-generated method stub
		CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
		cqlQuery.setQuery("update registry set 'STATUS' = 'ON' WHERE KEY = " + key + " and CLID = " + clId + " and index = " + index);
		QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
		
	}
	
	public static void releaseLock(Keyspace keyspace, String key, String cLID,
			int index) {
		// TODO Auto-generated method stub
		CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
		cqlQuery.setQuery("update registry SET STATUS = 'OFF' WHERE KEY = " + key + " and CLID = " + cLID + " and index = " + index);
		QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
	}


	public static void removeregistry(Keyspace keyspace, String key, String cLID,
			int index) {
		// TODO Auto-generated method stub
		CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
		cqlQuery.setQuery("delete from registry WHERE KEY = " + key + " and CLID = " + cLID + " and index = " + index);
		QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();
		
	}
	
	public static void requestLock(Keyspace keyspace, String key, String cLID, int index) {
		// TODO Auto-generated method stub
		setRegister(key, index, cLID, keyspace);
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	*/
	
}
