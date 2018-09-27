package com.leo.servlet;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.leo.ranker.*;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;

/*
 * Taking in JSON from MapReduceAPI as:
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
 * Sending out JSON to GCP Vision API (POST) as:
 * {
 *   	"requests": 
 *       [{"image": {"source": {"imageUri": "aaa.jpg"}},
 *         "features": 
 *           [{"type": "LABEL_DETECTION"}]
 *       }]
 * }
 * 
 * Taking response from GCP Vision API POST as:
 * {
 *   "responses": 
 *      [{"labelAnnotations": 
 *          [{"mid": "/m/0bt9lr",
 *            "description": "dog",
 *            "score": 0.8920206,
 *            "topicality": 0.8920206},
 *            {"mid": "/m/01z5f",
 *             "description": "dog like mammal",
 *             "score": 0.88694125,
 *             "topicality": 0.88694125}]
 *       }]
 * }
 * 
 * Returning response to MapReduceAPI as:
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
 *     {"word" : "football"}],
 *  key : 1
 * }
 *     
 */


@WebServlet("/labelranker")
public class LabelRankerApiServlet extends HttpServlet{
	private static final long serialVersionUID = 1L;
	private String APIKey = "xxxxxxx";
	
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, FileNotFoundException, IOException{
		String payloadRequest = getPayload(request);
		
		JSONParser parser = new JSONParser();
		JSONObject reqJSON = null;
		try {
			reqJSON = (JSONObject) parser.parse(payloadRequest);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}		
		List<String> imgURLForPost = populateImgUriList(reqJSON);
		List<String> words = populateWordList(reqJSON);
		long longKey = (Long)reqJSON.get("key");
		int key = new Long(longKey).intValue();
		
		CloseableHttpClient client = HttpClients.createDefault();
		HttpPost httpPost = new HttpPost("https://vision.googleapis.com/v1/images:annotate?key=" + APIKey);
		httpPost.setHeader("ACCEPT", "application/json");
		httpPost.addHeader("CONTENT-TYPE", "application/json");
		
		HashMap<String, List<String>> labelJsonMap = new HashMap<>();
		for(int i = 0; i < imgURLForPost.size(); i++){
			StringEntity ent = new StringEntity(makeGCPPayload(imgURLForPost.get(i)));
			httpPost.setEntity(ent);
			HttpResponse resp = client.execute(httpPost);
			HttpEntity entity = resp.getEntity();
			String labelJson = EntityUtils.toString(entity, "UTF-8");
			List<String> labelList = null;
			try {
				labelList = getLabelList(labelJson);
			} catch (ParseException e) {
				e.printStackTrace();
			}
			labelJsonMap.put(imgURLForPost.get(i), labelList);		
		}
		
		LabelRanker ranker = new LabelRanker();
		List<LabelRelevance> labelRelevanceList = ranker.rank(labelJsonMap, words);
		JSONObject returnPayload = constructReturnPayload(labelRelevanceList, words, key);		
		
		response.setContentType("application/json");
		response.getWriter().println(returnPayload.toString());
	}
	
	@SuppressWarnings("unchecked")
	private JSONObject constructReturnPayload(List<LabelRelevance> labelList, List<String> wordsList, int key){
		JSONObject returnPayload = new JSONObject();

		JSONArray img_urlArr = new JSONArray();
		for(int i = 0; i < labelList.size(); i++){
			LabelRelevance obj = labelList.get(i);
			
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
		returnPayload.put("key", key);
				
		return returnPayload;
	}
		
		/* 
		 * Use array indices to get the words
		 * {
		 * 	img_url: [
		 * 		{
		 * 			"url1" : "xxx",
		 * 			"labels" : [
		 * 				{ 
		 * 					"label" : "sss"
		 * 				},
		 *        {
		 *          "label" : "ttt"
		 *        },
		 *        {
		 *          "label" : "uuu"
		 *        }
		 *      ]
		 *    }, 
		 *    {
		 * 			"url2" : "xxx",
		 * 			"labels" : [
		 * 				{ 
		 * 					"label" : "sss"
		 * 				},
		 *        {
		 *          "label" : "ttt"
		 *        },
		 *        {
		 *          "label" : "uuu"
		 *        }
		 *      ]
		 *    } 
		 *  ],
		 *  word_list: [
		 *    {
		 *      "word" : "cat"
		 *    },
		 *    {
		 *      "word" : "pizza"
		 *    },
		 *    {
		 *      "word" : "football"
		 *    }
		 *  ]
		 *}
		 */
		
	private List<String> getLabelList(String labelJson) throws ParseException{
		JSONParser parser = new JSONParser();
		JSONObject json = (JSONObject) parser.parse(labelJson);		
		JSONArray responsesArr = (JSONArray) json.get("responses");
		JSONObject labelAnnotObj = (JSONObject) responsesArr.get(0);
		JSONArray labelAnnotationsArr = (JSONArray) labelAnnotObj.get("labelAnnotations");
		List<String> labels = new ArrayList<>();
		for(int i = 0; i < labelAnnotationsArr.size(); i++){
			JSONObject obj = (JSONObject) labelAnnotationsArr.get(i);
			labels.add(obj.get("description").toString());			
		}
		return labels;	
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
		
	
	@SuppressWarnings("unchecked")
	private String makeGCPPayload(String url){
		JSONObject payload = new JSONObject();
		
		JSONObject imageUri = new JSONObject();
		imageUri.put("imageUri", url);
		
		JSONObject source = new JSONObject();
		source.put("source", imageUri);
		
		JSONObject type = new JSONObject();
		type.put("type", "LABEL_DETECTION");
		
		JSONArray featuresArr = new JSONArray();
		featuresArr.add(type);
		
		JSONObject image = new JSONObject();
		image.put("image", source);
		image.put("features", featuresArr);
		
		JSONArray requestArr = new JSONArray();
		requestArr.add(image);
		
		payload.put("requests", requestArr);
		
		return payload.toString();
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
