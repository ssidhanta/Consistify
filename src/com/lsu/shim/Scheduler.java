package com.lsu.shim;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.HashMap;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.lsu.objects.RegistryEntry;

public class Scheduler {

	public boolean checkschedulability() {
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
	
	public String schedule(CopyOnWriteArrayList<RegistryEntry> rList) {
		// TODO Auto-generated method stub
		String tempRet=null;
		long min =999999;
		RegistryEntry r = null;
		for(int i=0;i<rList.size();i++)
		{ 
			r = rList.get(i);
			//System.out.println("***min:="+ min  +"(r.getDEADLINE():="+r.getDEADLINE()+" r.getWAITINGTIME():="+r.getWAITINGTIME().trim()+" (r.getLATENCYDEP():="+r.getLATENCYDEP());
			if(r!=null && min>(Long.parseLong(r.getDEADLINE().trim()) - (Long.parseLong(r.getCREATETIME().trim()) - (int)System.currentTimeMillis()) -Integer.parseInt(r.getWAITINGTIME().trim())-Long.parseLong(r.getLATENCYDEP().trim())))
			{
				min = Long.parseLong(r.getDEADLINE().trim())-Integer.parseInt(r.getWAITINGTIME().trim())-Long.parseLong(r.getLATENCYDEP().trim());
				//System.out.println("**111*CLID:=="+r.getCLID()+" clId:="+r.getSTATUS());
				tempRet = r.getCLID() + ":" + r.getSTATUS();
			}	
		}
		return tempRet;
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
	
	public void requestLock(Session session, String key, String cLID, int index) {
		// TODO Auto-generated method stub
		setRegister(key, index, cLID, session);
		
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	
}
