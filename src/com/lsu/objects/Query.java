package com.lsu.objects;

public class Query {
	private String index;
	private long latency;
	private String guarantee;
	private String key;
	private String CLID;
	private String consistencyLevel;
	
	public String getConsistencyLevel() {
		return consistencyLevel;
	}
	public void setConsistencyLevel(String consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
	}
	public String getCLID() {
		return CLID;
	}
	public void setCLID(String cLID) {
		CLID = cLID;
	}
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public String getIndex() {
		return index;
	}
	public void setIndex(String string) {
		this.index = string;
	}
	public long getLatency() {
		return latency;
	}
	public void setLatency(long latency) {
		this.latency = latency;
	}
	public String getGuarantee() {
		return guarantee;
	}
	public void setGuarantee(String guarantee) {
		this.guarantee = guarantee;
	}

}
