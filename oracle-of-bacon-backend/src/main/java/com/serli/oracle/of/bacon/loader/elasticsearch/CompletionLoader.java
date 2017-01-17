package com.serli.oracle.of.bacon.loader.elasticsearch;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import io.searchbox.client.JestClient;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;

public class CompletionLoader {
    private static AtomicInteger count = new AtomicInteger(0);

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Expecting 1 arguments, actual : " + args.length);
            System.err.println("Usage : completion-loader <actors file path>");
            System.exit(-1);
        }

        String inputFilePath = args[0];
        JestClient client = ElasticSearchRepository.createClient();
        
        // Creating a bulk to execute all insertions at the end
        Builder bulkBuiler = new Bulk.Builder();
        
        try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
            bufferedReader.lines()
                    .forEach(line -> {
                    	// Adding each line to the bulk
                        String actorLine = "{\"name\" : " + line + "}";
                    	Index index = new Index.Builder(actorLine).index("person").type("actor").build();
                    	bulkBuiler.addAction(index);
                    	
                    	System.out.println(line);
                    });
        }
        
        // Launching the insertion of actors
    	client.execute(bulkBuiler.build());

        System.out.println("Inserted total of " + count.get() + " actors");
    }
}
