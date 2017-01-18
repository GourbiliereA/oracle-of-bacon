package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.mapping.PutMapping;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);
    private static List<Index> tempActors;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();
        
        // Creating the index
        createPersonIndex(client);
        
        // Creating the mapping on actors to do the completion
        createActorMapping(client);
        
        // Creating a list to execute request all 100000 actors with a bulk
        tempActors = new ArrayList<Index>();
        
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines()
                    .forEach(line -> {
                    	int actualCount = count.incrementAndGet();
                    	
                    	// Setting the tags for the suggestion of actors
                    	List<String> tagsList = createTags(line);
                    	
                    	// Adding each line to the bulk
                    	try {
							tempActors.add(getActorIndex(line, tagsList));
						} catch (Exception e) {
							System.out.println("Error when creating actor " + line +".\n" + e.getMessage());
						}
                    	
                    	if (actualCount % 100000 == 0) {
                            // Launching the insertion of 100000 actors
                            Builder bulkBuiler = new Bulk.Builder();
                        	bulkBuiler.addAction(tempActors);
                        	try {
								client.execute(bulkBuiler.build());
							} catch (Exception e) {
								System.out.println("Error when inserting actors with ElasticSearch.\n" + e.getMessage());
							}
                        	// Clearing the list of actors
                        	tempActors = new ArrayList<Index>();
                    	}
                    	
                    	System.out.println(line);
                    });
        }
        
        // Launching the last insertion of actors
        Builder bulkBuiler = new Bulk.Builder();
    	bulkBuiler.addAction(tempActors);
    	client.execute(bulkBuiler.build());

        System.out.println("Inserted total of " + count.get() + " actors");
    }
    
    private static void createPersonIndex(JestClient client) throws IOException {
    	CreateIndex index = new CreateIndex.Builder(ElasticSearchRepository.INDEX_NAME).build();
    	client.execute(index);
    }
    
    private static void createActorMapping(JestClient client) throws IOException {
    	StringBuffer mappingText = new StringBuffer()
    			.append("{")
    			.append("	\""+ElasticSearchRepository.TYPE_NAME+"\": {")
    			.append("		\"properties\": {")
    			.append("			\"name\": {")
    			.append("				\"type\": \"string\"")
    			.append("			},")
    			.append("			\"tag_suggest\": {")
    			.append("				\"type\": \"completion\"")
    			.append("			}")
    			.append("		}")
    			.append("	}")
    			.append("}");
    			
    	PutMapping mapping = new PutMapping.Builder(ElasticSearchRepository.INDEX_NAME, ElasticSearchRepository.TYPE_NAME, mappingText.toString()).build();
    	
    	client.execute(mapping);
    }
    
    private static Index getActorIndex(String actor, List<String> tags) throws IOException {
        StringBuffer actorLine = new StringBuffer()
        		.append("{")
        		.append("	\"name\" : " + actor + ",")
        		.append("	\"tag_suggest\" : {")
        		.append(		"\"input\" : " + tags.toString())
        		.append(	"}")
        		.append("}");
        
    	Index index = new Index.Builder(actorLine.toString())
    			.index(ElasticSearchRepository.INDEX_NAME)
    			.type(ElasticSearchRepository.TYPE_NAME)
    			.build();
    	
    	return index;
    }
    
    private static List<String> createTags (String actor) {
    	List<String> tagsList = new ArrayList<String>();
    	
    	tagsList.add(actor);
    	String lineWithoutQuotes = actor.substring(1);
    	lineWithoutQuotes = lineWithoutQuotes.substring(0, lineWithoutQuotes.length()-1);
    	String lineWithoutQuotesAndComma = lineWithoutQuotes.replace(",", "");
    	String[] lineWords = lineWithoutQuotesAndComma.split(" ");
    	
    	// Adding to tags combinations of words
    	for (int i = 0 ; i < lineWords.length ; i++) {
    		String tag = lineWords[i];
    		tagsList.add("\"" + tag + "\"");
    		
    		for (int j = i + 1 ; j < lineWords.length ; j++) {
    			tag = tag + " " + lineWords[j];
        		tagsList.add("\"" + tag + "\"");
    		}
    	}
    	
    	return tagsList;
    }
}
