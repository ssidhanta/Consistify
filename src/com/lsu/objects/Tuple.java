package com.lsu.objects;

import java.util.concurrent.ArrayBlockingQueue;

import com.datastax.driver.core.Session;
import com.lsu.shim.DependencyChecker;

public class Tuple {
	private Session session;
	private ArrayBlockingQueue<Query> causalQueue;
	private String result;
	
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	
	public Session getSession() {
		return session;
	}
	public void setSession(Session session) {
		this.session = session;
	}
	public ArrayBlockingQueue<Query> getCausalQueue() {
		return causalQueue;
	}
	public void setCausalQueue(ArrayBlockingQueue<Query> causalQueue) {
		this.causalQueue = causalQueue;
	}
	
}
