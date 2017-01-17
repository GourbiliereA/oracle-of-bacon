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
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class ElasticSearchRepository {

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
        		.append("	\"query\" : {")
        		.append("		\"match\": {")
        		.append("			\"name\": \"" + searchQuery + "\"")
        		.append(" 	}")
        		.append("	},")
        		.append("	\"suggest\" : {")
        		.append("		\"my-suggestion\" : {")
        		.append("			\"text\" : \"" + searchQuery + "\",")
        		.append("			\"term\" : {")
        		.append("  			\"field\" : \"name\"")
        		.append("			}")
        		.append("		}")
        		.append("	}")
        		.append("}");
      
        Search search = (Search) new Search.Builder(query.toString())
        .addIndex("person")
        .addType("actor")
        .build();
        
        SearchResult searchResult = null;
    	try {
    		// Execute the request
			searchResult = jestClient.execute(search);
		} catch (IOException e) {
			System.out.println("Error when executing ElastingSearch's request.\n" + e.getMessage());
		}

    	JsonObject result = null;
    	if (searchResult != null) {
    		result = searchResult.getJsonObject();
    	}
    	
    	// Put the actors found in the array of suggestions
    	JsonArray jsonArray = result.getAsJsonObject("hits").getAsJsonArray("hits");
    	for (JsonElement actor : jsonArray) {
    		String actorName = actor.getAsJsonObject().getAsJsonObject("_source").get("name").getAsString();
    		suggestList.add(actorName);
    	}
    	
        return suggestList;
    }

    public JestClient getJestClient() {
    	return jestClient;
    }
}
