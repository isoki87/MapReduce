package com.leo.ranker;

import java.util.List;

public class LabelRelevance {
	List<String> labels;
	String uri;
	int relevance;
	
	LabelRelevance(List<String> labels, int relevance, String uri){
		this.labels = labels;
		this.relevance = relevance;
		this.uri = uri;
	}		
	
	public List<String> getLabels(){
		return this.labels;
	}
	
	public String getUri(){
		return this.uri;
	}
	
	public int getRelevance(){
		return this.relevance;
	}
}
