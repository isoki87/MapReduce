package com.leo.mapreduce;

import java.util.List;

public class ImgData {
	List<String> labels;
	String uri;
	public int relevance;
	
	public ImgData(List<String> labels, int relevance, String uri){
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
