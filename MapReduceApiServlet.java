package com.leo.servlet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.simple.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.leo.mapreduce.ImgData;

/*
 * Taking in JSON from front-end as:
 * {img_url:
 *   [{url : aaa.jpg},
 *   {url : bbb.jpg},
 *   {url : ccc.jpg}],
 *  word_list:
 *   [{word : football},
 *   {word : chocolate},
 *   {word : test}]
 * }
 * 
 * Sending out JSON to each mapper function (POST) as:
 * {img_url:
 *   [{url : aaa.jpg},
 *   {url : bbb.jpg},
 *   {url : ccc.jpg}],
 *  word_list:
 *   [{word : football},
 *   {word : chocolate},
 *   {word : test}], 
 *  key : 1
 * }
 * 
 * Taking response from mapper function via POST as:
 * {img_url: 
 *    [{"url1" : "xxx",
 *	  	"labels" : 
 *        [{"label" : "sss"},
 *        {"label" : "ttt"},
 *        {"label" : "uuu"}],
 *      "relevance" : 1
 *     }, 
 *     {"url2" : "xxx",
 * 		  "labels" : 
 *        [{"label" : "sss"},
 *         {"label" : "ttt"},
 *         {"label" : "uuu"}],
 *      "relevance" : 2
 *     }],
 *  word_list: 
 *    [{"word" : "cat"},
 *     {"word" : "pizza"},
 *     {"word" : "football"}
 *    ],
 *  key : 1
 * }
 * 
 * Returning response to front-end as:
 * {img_url: 
 *    [{"url1" : "xxx",
 *	  	"labels" : 
 *        [{"label" : "sss"},
 *        {"label" : "ttt"},
 *        {"label" : "uuu"}]
 *     }, 
 *     {"url2" : "xxx",
 * 		  "labels" : 
 *        [{"label" : "sss"},
 *         {"label" : "ttt"},
 *         {"label" : "uuu"}]
 *     }],
 *  word_list: 
 *    [{"word" : "cat"},
 *     {"word" : "pizza"},
 *     {"word" : "football"}
 *    ],
 * } 
 *     
 */

@WebServlet("/mapreduce")
public class MapReduceApiServlet extends HttpServlet{
	private static final long serialVersionUID = 1L;
	
	//max represents the number of mapper servlets available
	int max = 2;

	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, FileNotFoundException, IOException {
		//Turns request JSON from front-end into a string
		String frontEndRequest = getPayload(request);
		
		//Parses the front end JSON into objects
		JSONParser parser = new JSONParser();
		JSONObject reqJSON = null;
		try {
			reqJSON = (JSONObject) parser.parse(frontEndRequest);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}		
		
		//Obtains all image URL's and words from request JSON
		List<String> imgURLForPost = populateImgUriList(reqJSON);
		List<String> words = populateWordList(reqJSON);
		
		//Partitions the URL list for each mapper
		//The Key will determine the mapper address to call
		//The Key will also determine the section of the uri list for the mapper
		//The Key will be 0-2 (number of servlets available)
		int key = 0;
		//Carries will determine how many of the servlets will have 1 more uri than the rest
		int uriCount = imgURLForPost.size();
		int carries = uriCount % max;
		int mapperSize = uriCount / (max + 1);
		ExecutorService threadPool = Executors.newFixedThreadPool(max + 1);
		ImgData[] imgArr = new ImgData[uriCount];
		
		
		while(key <= max){
			//Wrap each smaller sized bits into JSON to send out
			JSONObject mapperPayload = constructMapperPayload(imgURLForPost, words, key, carries);
			String mapperAddress = null;
			
			switch(key){
				//Each case will have its own POST request to send to
				case 0: mapperAddress = "http://helloleolin-labelranker-1.us-east-2.elasticbeanstalk.com/labelranker";
							  break;
				case 1: mapperAddress = "http://helloleolin-labelranker-2.us-east-2.elasticbeanstalk.com/labelranker";
					      break;
				case 2: mapperAddress = "http://helloleolin-labelranker-3.us-east-2.elasticbeanstalk.com/labelranker";
							  break;
			}			
			
			CloseableHttpClient client = HttpClients.createDefault();
			HttpPost httpPost = new HttpPost(mapperAddress);
			httpPost.setHeader("ACCEPT", "application/json");
			httpPost.addHeader("CONTENT-TYPE", "application/json");
			
			StringEntity ent = new StringEntity(mapperPayload.toString());
			httpPost.setEntity(ent);
			HttpResponse resp = client.execute(httpPost);
			HttpEntity entity = resp.getEntity();
			String mapperReturnPayload = EntityUtils.toString(entity, "UTF-8");
			
			//From the response, unpackage each item into an ImgData object
			//Put the ImgData object in order, with the starting spot derived from key/carries/max
			populateMasterArr(imgArr, mapperReturnPayload, carries, mapperSize);			
			key++;
		}				
		
		threadPool.shutdown();
		try {
			if(!threadPool.awaitTermination(1000, TimeUnit.MILLISECONDS)){
				threadPool.shutdownNow();
			}
		} catch(InterruptedException e){
			threadPool.shutdownNow();
		}
		
		//Sort the ImgData[] with a comparator for relevance
		Arrays.sort(imgArr, new Comparator<ImgData>(){
			public int compare(ImgData a, ImgData b){
				return (a.relevance == b.relevance) ? 0 : ((a.relevance < b.relevance) ? 1 : -1);		
			}				
		});		
		

		//Turn the ImgData[] into a JSON to be sent back to the front-end
		JSONObject frontEndReturnPayload = constructFrontEndReturnPayload(imgArr, words);
		
		response.setContentType("application/json");
		response.getWriter().println(frontEndReturnPayload.toString());
	}
	
	private void populateMasterArr(ImgData[] imgArr, String mapperReturnPayload, int carries, int mapperSize){
		JSONParser parser = new JSONParser();
		JSONObject obj = null;
		try {
			obj = (JSONObject) parser.parse(mapperReturnPayload);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}		
		
		int key = Integer.parseInt((String)obj.get("key"));
		int start = 0;
		int len = 0;
		
		if(key < carries){
			len = mapperSize + 1;
			start = key * len;
		} else {
			len = mapperSize;
			start = (key * len) + carries;
		}
		
		JSONArray objArr = (JSONArray) obj.get("img_url");
		for(int i = 0; i < objArr.size(); i++){
			List<String> currLabels = getLabelsList((JSONObject)objArr.get(i));
			int currRelevance = getRelevance((JSONObject)objArr.get(i));
			String currURL = getURL((JSONObject)objArr.get(i));	
			
			imgArr[start] = new ImgData(currLabels, currRelevance, currURL);
			start++;
		}
	}
	
	private List<String> getLabelsList(JSONObject obj){
		List<String> labels = new ArrayList<>();
		JSONArray labelsList = (JSONArray) obj.get("labels");
		for(int i = 0; i < labelsList.size(); i++){
			JSONObject currLabel = (JSONObject) labelsList.get(i);
			labels.add((String)currLabel.get("label"));
		}		
		return labels;
	}
	
	private String getURL(JSONObject obj){
		return (String)obj.get("url");
	}
	
	private int getRelevance(JSONObject obj){
		return Integer.parseInt((String)obj.get("relevance"));
	}
	
	@SuppressWarnings("unchecked")
	private JSONObject constructFrontEndReturnPayload(ImgData[] imgArr, List<String> wordsList){
		JSONObject returnPayload = new JSONObject();

		JSONArray img_urlArr = new JSONArray();
		for(int i = 0; i < imgArr.length; i++){
			ImgData obj = imgArr[i];
			
			JSONObject url = new JSONObject();
			url.put("url", obj.getUri());
			
			List<String> gcpLabels = obj.getLabels();
			JSONArray labelsArr = new JSONArray();
			for(int j = 0; j < gcpLabels.size(); j++){
				JSONObject label = new JSONObject();
				label.put("label", gcpLabels.get(j));
				labelsArr.add(label);
			}
			url.put("labels", labelsArr);
			img_urlArr.add(url);
			url.put("relevance", obj.getRelevance());
		}
		
		JSONArray wordsArr = new JSONArray();
		for(int i = 0; i < wordsList.size(); i++){
			JSONObject word = new JSONObject();
			word.put("word", wordsList.get(i));
			wordsArr.add(word);			
		}
		
		returnPayload.put("img_url", img_urlArr);
		returnPayload.put("word_list", wordsArr);	
				
		return returnPayload;
	}
		
	
	@SuppressWarnings("unchecked")
	private JSONObject constructMapperPayload(List<String> uriList, List<String> wordsList, int key, int carries){
		JSONObject payload = new JSONObject();

		JSONArray img_urlArr = new JSONArray();
		
		//The img lists has been partitioned and split into smaller pieces
		int len = 0;
		int start = 0;
		if(key < carries){
			len = (uriList.size() / (max + 1)) + 1;
			start = key * len;
			for(int i = 0; i < len; i++){
				JSONObject url = new JSONObject();
				url.put("url", uriList.get(start + i));
				img_urlArr.add(url);
			}
		} else {
			len = uriList.size() / (max + 1);
			start = (key * len) + carries;
			for(int i = 0; i < len; i++){
				JSONObject url = new JSONObject();
				url.put("url", uriList.get(start + i));
				img_urlArr.add(url);
			}
		}
		
		JSONArray wordsArr = new JSONArray();
		for(int i = 0; i < wordsList.size(); i++){
			JSONObject word = new JSONObject();
			word.put("word", wordsList.get(i));
			wordsArr.add(word);			
		}
		
		payload.put("img_url", img_urlArr);
		payload.put("word_list", wordsArr);
		payload.put("key", key);
		
		return payload;
	}
	
	private List<String> populateWordList(JSONObject reqJSON){
		JSONArray wordList = (JSONArray) reqJSON.get("word_list");
		List<String> words = new ArrayList<>();
		JSONObject word = new JSONObject();
		for(int i = 0; i < wordList.size(); i++){
			word = (JSONObject) wordList.get(i);
			words.add((String) word.get("word"));
		}
		return words;
	}
	
	private List<String> populateImgUriList(JSONObject reqJSON){
		JSONArray imgURL = (JSONArray) reqJSON.get("img_url");
		List<String> imgURLForPost = new ArrayList<>();
		JSONObject url = new JSONObject();
		for(int i = 0; i < imgURL.size(); i++){
			url = (JSONObject) imgURL.get(i);
			imgURLForPost.add((String) url.get("url"));
		}
		return imgURLForPost;
	}
	
	private String getPayload(HttpServletRequest request) throws IOException {
		String payload = null;
		StringBuilder sb = new StringBuilder();
		BufferedReader br = null;
		try{
			InputStream is = request.getInputStream();
			if(is != null){
				br = new BufferedReader(new InputStreamReader(is));
				char[] charBuffer = new char[128];
				int bytesRead = -1;
				while((bytesRead = br.read(charBuffer)) > 0){
					sb.append(charBuffer, 0, bytesRead);
				}
			} else {
				sb.append("");
			}
		} catch (IOException e){
			throw e;
		} finally {
			if(br != null){
				try {
					br.close();
				} catch (IOException e){
					throw e;
				}
			}
		}
		payload = sb.toString();		
		return payload;
	}
}
