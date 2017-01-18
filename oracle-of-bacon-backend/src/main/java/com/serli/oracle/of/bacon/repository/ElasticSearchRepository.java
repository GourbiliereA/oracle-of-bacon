package com.serli.oracle.of.bacon.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Suggest;
import io.searchbox.core.SuggestResult;

public class ElasticSearchRepository {
    
    public static final String INDEX_NAME = "person";
    public static final String TYPE_NAME = "actor";

    private final JestClient jestClient;

    public ElasticSearchRepository() {
        jestClient = createClient();

    }

    public static JestClient createClient() {
        JestClient jestClient;
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .readTimeout(60000)
                .build());

        jestClient = factory.getObject();
        return jestClient;
    }

    public List<String> getActorsSuggests(String searchQuery) {
    	List<String> suggestList = new ArrayList<String>();        

        StringBuffer query = new StringBuffer()
        		.append("{")
        		.append("	\"actor\" : {")
        		.append("		\"text\" : \"" + searchQuery + "\",")
        		.append("		\"completion\" : {")
        		.append("  			\"field\" : \"tag_suggest\"")
        		.append("		}")
        		.append("	}")
        		.append("}");
      
        Suggest suggest = (Suggest) new Suggest.Builder(query.toString())
        .addIndex(INDEX_NAME)
        .build();
        
        SuggestResult suggestResult = null;
    	try {
    		// Execute the request
    		suggestResult = jestClient.execute(suggest);
		} catch (IOException e) {
			System.out.println("Error when executing ElastingSearch's request.\n" + e.getMessage());
		}
    	
    	JsonObject result = null;
    	if (suggestResult != null) {
    		result = suggestResult.getJsonObject();
    	}
    	
    	// Put the actors found in the array of suggestions
    	JsonObject jsonObject = (JsonObject) result.getAsJsonArray("actor").get(0);
    	JsonArray actors = jsonObject.get("options").getAsJsonArray();
    	for (JsonElement actor : actors) {
    		String actorName = actor.getAsJsonObject().getAsJsonObject("_source").get("name").getAsString();
    		suggestList.add(actorName);
    	}
    	
        return suggestList;
    }

    public JestClient getJestClient() {
    	return jestClient;
    }
}
