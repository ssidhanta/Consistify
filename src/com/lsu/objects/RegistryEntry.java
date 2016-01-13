package com.lsu.objects;

import me.prettyprint.hector.api.factory.HFactory;

public class RegistryEntry {
	private String key;
	private String CLID;
	private String INDEX;
	private String WAITINGTIME;
	private  String LATENCYDEP;
	private String DEADLINE;
	private String STATUS;
	private String CREATETIME;
	
	public String getCREATETIME() {
		return CREATETIME;
	}
	public void setCREATETIME(String cREATETIME) {
		CREATETIME = cREATETIME;
	}
	public String getKey() {
		return this.key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getCLID() {
		return this.CLID;
	}
	public void setCLID(String cLID) {
		CLID = cLID;
	}
	public String getINDEX() {
		return INDEX;
	}
	public void setINDEX(String iNDEX) {
		INDEX = iNDEX;
	}
	public String getWAITINGTIME() {
		return WAITINGTIME;
	}
	public  void setWAITINGTIME(String wAITINGTIME) {
		WAITINGTIME = wAITINGTIME;
	}
	public String getLATENCYDEP() {
		return LATENCYDEP;
	}
	public void setLATENCYDEP(String lATENCYDEP) {
		LATENCYDEP = lATENCYDEP;
	}
	public String getDEADLINE() {
		return DEADLINE;
	}
	public void setDEADLINE(String dEADLINE) {
		DEADLINE = dEADLINE;
	}
	public String getSTATUS() {
		return STATUS;
	}
	public void setSTATUS(String sTATUS) {
		STATUS = sTATUS;
	}
	
}
