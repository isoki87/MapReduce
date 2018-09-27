package com.leo.ranker;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;

public class LabelRanker {
	public List<LabelRelevance> rank(HashMap<String, List<String>> labelList, List<String> words){
		HashSet<String> wordsSet = new HashSet<>();
		for(int i = 0; i < words.size(); i++){
			wordsSet.add(words.get(i));
		}
		
		List<LabelRelevance> relevanceList = new ArrayList<>();
		for(Map.Entry<String, List<String>> entry : labelList.entrySet()){
			int relevance = findRelevance(entry.getValue(), wordsSet);
			relevanceList.add(new LabelRelevance(entry.getValue(), relevance, entry.getKey()));			
		}
		
		//Want largest number to be in the front
		Collections.sort(relevanceList, new Comparator<LabelRelevance>(){
			public int compare(LabelRelevance a, LabelRelevance b){
				return (a.relevance == b.relevance) ? 0 : ((a.relevance < b.relevance) ? 1 : -1);		
			}				
		});
			
		return relevanceList;
	}
	
	private int findRelevance(List<String> labels, HashSet<String> words){
		int relevance = 0;
		HashSet<String> visited = new HashSet<>();
		for(int i = 0; i < labels.size(); i++){
			if(!visited.contains(labels.get(i)) && words.contains(labels.get(i))){
				relevance++;
				visited.add(labels.get(i));
			}
		}	
		return relevance;
	}
}