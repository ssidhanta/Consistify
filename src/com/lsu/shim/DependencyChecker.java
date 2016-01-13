package com.lsu.shim;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;

import com.lsu.objects.Query;
import com.lsu.objects.Tuple;
import com.lsu.test.ClientQueryExampleThread;

public class DependencyChecker {
	
	public static CopyOnWriteArrayList<String> queryList = new CopyOnWriteArrayList();
	
	public static CopyOnWriteArrayList<Query> eventualList = new CopyOnWriteArrayList();
	
	public static CopyOnWriteArrayList<Query> causalList = new CopyOnWriteArrayList();
	
	public static CopyOnWriteArrayList<Query> serializeList = new CopyOnWriteArrayList();
	
	private int queryCount = 0;

	
	public CopyOnWriteArrayList<String> getQueryList() {
		return queryList;
	}

	public void setQueryList(CopyOnWriteArrayList<String> queryList) {
		this.queryList = queryList;
	}

	public CopyOnWriteArrayList<Query> getEventualList() {
		return eventualList;
	}

	public void setEventualList(CopyOnWriteArrayList<Query> eventualList) {
		this.eventualList = eventualList;
	}

	public CopyOnWriteArrayList<Query> getCausalList() {
		return causalList;
	}

	public void setCausalList(CopyOnWriteArrayList<Query> causalList) {
		this.causalList = causalList;
	}

	public CopyOnWriteArrayList<Query> getSerializeList() {
		return serializeList;
	}

	public void setSerializeList(CopyOnWriteArrayList<Query> serializeList) {
		this.serializeList = serializeList;
	}

	public int getQueryCount() {
		return queryCount;
	}

	public void setQueryCount(int queryCount) {
		this.queryCount = queryCount;
	}

	public static void setLatency(String queryStr, Query q){
		if(queryStr.contains("read"))
			   q.setLatency(Interface.latency_read);
		  else if(queryStr.contains("insert"))
			   q.setLatency(Interface.latency_insert);   
		   else if(queryStr.contains("update"))
			   q.setLatency(Interface.latency_update);
	}
	
	public static long getLatency(String queryStr, Query q){
		long latency = 0;
		if(queryStr.contains("read"))
			latency = Interface.latency_read;
		   else if(queryStr.contains("insert"))
			   latency = Interface.latency_insert;   
		   else if(queryStr.contains("update"))
			   latency = Interface.latency_update;
			return latency;
	}
	
	public static synchronized void getGuarantee(String condn,String filename, String CLID, long tInv){
		String queryStr=null;//Eventual=E, Causal=C, Serializability=S
		CopyOnWriteArrayList<String> operatorList = new CopyOnWriteArrayList<String>();
		CopyOnWriteArrayList<String> operandList = new CopyOnWriteArrayList<String>();
		//DependencyChecker dependencyChecker = new DependencyChecker();
		int index = 0;
		boolean readFlag = false, existSer = false;;
		//List<String> queryList = new ArrayList();
		StringTokenizer st = new StringTokenizer(condn, "+-/*><", true);
		BufferedReader br = null;
		String[] cArr= null;
		//HashMap<String,String> map = null;
		Query q = null;
		while (st.hasMoreTokens()) {
		    String token = st.nextToken();
		    if ("+-/*><".contains(token)) {
		       operatorList.add(token);
		    } else {
		       operandList.add(token);
		    }
		 }
		//for(int i=0;i<operandList.size();i++)
			//System.out.println("***operandList size:="+operandList.get(i));
		//System.out.println("***000filename:="+filename);
		try {
			String line;
			br = new BufferedReader(new FileReader(filename));
			//System.out.println("***filename:---"+filename);
			synchronized(DependencyChecker.queryList){
				while ((line=br.readLine()) != null){// && !line.contains("}")){// && readFlag) {
			
					//System.out.println("****br.readLine():="+line);
					//if(readFlag && !line.contains("}")){
					if(readFlag && line.contains("totalTime"))
					{
						readFlag=false;
					}
					if(readFlag && line.trim().contains("intrface."))
					{
						 //System.out.println("***readFlag Interface line="+line);
						 DependencyChecker.queryList.add(line.trim());
				    }
					if (line.contains("intrface.callInterface"))
					{
						readFlag = true;
					}
				}
			
			}
			//System.out.println("***queryList size:="+DependencyChecker.queryList.size());
			synchronized(DependencyChecker.serializeList){
				for(int i=DependencyChecker.queryList.size()-1; i>=0; i--)
				{
					//System.out.println("***queryList size iii:="+i);
					//System.out.println("***queryList size:="+queryList.get(i));
					if(DependencyChecker.queryList.get(i)!=null && DependencyChecker.queryList.get(i).trim().indexOf("\"")>=1){
						String[] valuesInDQuotes = StringUtils.substringsBetween(DependencyChecker.queryList.get(i) , "\"", "\"");
						//System.out.println("***valuesInDQuotes length:="+valuesInDQuotes.length);
					    if(valuesInDQuotes!=null && valuesInDQuotes.length>0){
					    	for(int j=1; j<valuesInDQuotes.length; j++)
						   {
					    	   //System.out.println("***valuesInDQuotes[j]:="+valuesInDQuotes[j]);
							   if(operandList.contains(valuesInDQuotes[j]))
							   {
								   //map =  new HashMap<String,String>();
								   //map.put(valuesInDQuotes[j], String.valueOf(i));
								   for(int x=0;x<DependencyChecker.serializeList.size();x++)
								   {
									  if(DependencyChecker.serializeList.get(x)!=null && DependencyChecker.serializeList.get(x).getIndex()!=null && String.valueOf(i).equalsIgnoreCase(DependencyChecker.serializeList.get(x).getIndex()))
									  {
										  existSer = true;
										 
									  }
								   }
								   if(!existSer)
								   {
									   q =  new Query();
									   q.setGuarantee("S");
									   //System.out.println("***111 String.valueOf(i):="+String.valueOf(i));
									   q.setIndex(String.valueOf(i));
									   setLatency(DependencyChecker.queryList.get(i),q);
									   q.setKey(valuesInDQuotes[j]);
									   q.setCLID(CLID);
									   //q.setLatency(latency);
									   DependencyChecker.serializeList.add(q);
								   }
								   //System.out.println("***111add to serializeList:="+q.getCLID());
								   for(int k=1; k<valuesInDQuotes.length; k++)
								   {
									   if(k!=j && valuesInDQuotes.length>1)
									   {	
										   //if(queryList.get(i)!=null && queryList.get(i).trim().indexOf("\"")>=2){
										   //String[] valuesInDQuotes1 = StringUtils.substringsBetween(queryList.get(k) , "\"", "\"");
										    for(int l=i-1; l>0 && DependencyChecker.queryList.get(l)!=null && DependencyChecker.queryList.get(l).trim().indexOf("\"")>=1; l--)
											{
												String[] valuesInDQuotes1 = StringUtils.substringsBetween(DependencyChecker.queryList.get(l) , "\"", "\"");
												if(valuesInDQuotes1!=null  && valuesInDQuotes1.length>l && valuesInDQuotes.length>k && valuesInDQuotes1[l]!=null && valuesInDQuotes[k]!=null && !"".equalsIgnoreCase(valuesInDQuotes1[l]) && !"".equalsIgnoreCase(valuesInDQuotes[k]) && valuesInDQuotes1[l].equalsIgnoreCase(valuesInDQuotes[k]))
												{
													//System.out.println("*** valuesInDQuotes1[l]:="+valuesInDQuotes1[l]);
													//System.out.println("*** valuesInDQuotes[k]:="+valuesInDQuotes[k]);
													//map =  new HashMap<String,String>();
													//map.put(valuesInDQuotes1[l], String.valueOf(l));
													for(int x=0;x<DependencyChecker.serializeList.size();x++)
													{
														  if(DependencyChecker.serializeList.get(x)!=null && DependencyChecker.serializeList.get(x).getIndex()!=null && String.valueOf(i).equalsIgnoreCase(DependencyChecker.serializeList.get(x).getIndex()))
														  {
															  existSer = true;
															  	
														  }
													}
													
													if(!existSer)
													{          
														q =  new Query();
														q.setGuarantee("S");
														//System.out.println("***222 String.valueOf(i):="+String.valueOf(i));
														q.setIndex(String.valueOf(i));
														setLatency(DependencyChecker.queryList.get(i),q);
														q.setKey(valuesInDQuotes1[l]);
														q.setCLID(CLID);
														DependencyChecker.serializeList.add(q);
													}
													//System.out.println("***222add to serializeList:="+q.getCLID());
												}
											}
										   //}
									  }
								   }
							   }
							}
					    }
					}
				}
			}
	
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch blockc
			e.printStackTrace();
		} 
		/*for(int x=0;x<DependencyChecker.serializeList.size();x++){
			System.out.println("***3qwdwd3d3 serializeList 11:="+DependencyChecker.serializeList.get(x).getIndex());
		}*/
		//System.out.println("***serializeList size:="+DependencyChecker.serializeList.size());
		synchronized(DependencyChecker.causalList){
			if(DependencyChecker.serializeList==null)
			{
				//System.out.println("***serializeList ==null");
				for(int i=0;i<operatorList.size();i++)
				{
					if(operatorList.get(i)!=null && "".equalsIgnoreCase(operatorList.get(i)))
					{
						if(operatorList.get(i).contains(">"))
						{
							cArr = condn.split(">");
							//causalList.add(condn.split(">")[1]);
							//guarantee = "C";
							//return guarantee;
						}
						else if(operatorList.get(i).contains("<"))
						{
							cArr = condn.split("<");
							//causalList.add(condn.split(">")[1]);
							//causalList.add(condn.split(">")[0]);
						}
						if(cArr.length>0){
							for(int m=0; m<cArr.length; m++)
							{	
								q =  new Query();
								q.setGuarantee("C");
								q.setIndex(String.valueOf(m));
								//setLatency(queryList[m],q);
								DependencyChecker.causalList.add(q);
							}
						}
					}
				}
				
			}
		}
		//System.out.println("***serializeList size:-"+DependencyChecker.serializeList.size());
	}
	
	public static String getGuarantee_old(String condn,String filename){
		String guarantee = "E", queryStr=null;//Eventual=E, Causal=C, Serializability=S
		CopyOnWriteArrayList<String> operatorList = new CopyOnWriteArrayList<String>();
		CopyOnWriteArrayList<String> operandList = new CopyOnWriteArrayList<String>();
		StringTokenizer st = new StringTokenizer(condn, "(?<=[-+*/])|(?=[-+*/><])", true);
		BufferedReader br = null;
		while (st.hasMoreTokens()) {
		    String token = st.nextToken();

		    if ("(?<=[-+*/])|(?=[-+*/><])".contains(token)) {
		       operatorList.add(token);
		    } else {
		       operandList.add(token);
		    }
		 }
		try {
			br = new BufferedReader(new FileReader(filename));
			while (br.readLine() != null) {
			   
			   String[] valuesInDQuotes = StringUtils.substringsBetween(br.readLine() , "\"", "\"");
			   if(valuesInDQuotes!=null && valuesInDQuotes.length>0){
				   for(int j=0; j<valuesInDQuotes.length; j++)
				   {
					   if(operandList.contains(valuesInDQuotes[j]))
					   {
						   guarantee="S";
						   return guarantee;
					   }
					   String[] valuesInQuotes = StringUtils.substringsBetween(valuesInDQuotes[j] , "\'", "\'");
					   if(valuesInQuotes!=null && valuesInQuotes.length>0){
					   for(int k=0; k<valuesInQuotes.length; k++)
						   {
							   if(operandList.contains(valuesInQuotes[k]))
							   {
								   guarantee="S";
								   return guarantee;
							   }
							      
						   }
					   }
				   }   
			   }
			   
		    }
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		for(int i=0;i<operandList.size();i++)
		{
			if(operandList.get(i)!=null && "".equalsIgnoreCase(operandList.get(i)))
			{
				if(operandList.get(i).contains(">") || operandList.get(i).contains("<"))
				{
					guarantee = "C";
					return guarantee;
				}
				
			}
				
			//System.out.println("operatorList:-"+operandList.get(i));
		}
		return guarantee;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//String condn = "price>20", className = "C:\\Users\\ssidha1\\workspace\\Consistify\\src\\com\\lsu\\test\\ClientQueryExampleThread.java";
		//ClientQueryExampleThread.callClient(cLevel, condn);
		//System.out.println("guarantere:-"+getGuarantee(condn,className));
		
	}

}
