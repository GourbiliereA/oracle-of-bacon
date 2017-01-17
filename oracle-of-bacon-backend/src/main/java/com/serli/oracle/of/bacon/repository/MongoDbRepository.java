package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import org.bson.Document;

public class MongoDbRepository {

    private final MongoClient mongoClient;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
    }

    public Document getActorByName(String name) {
    	MongoDatabase db = mongoClient.getDatabase("mongo-bacon");
    	MongoCollection<Document> actors = db.getCollection("actors");
        return actors.find(eq("name:ID", name)).first();
    }
}
