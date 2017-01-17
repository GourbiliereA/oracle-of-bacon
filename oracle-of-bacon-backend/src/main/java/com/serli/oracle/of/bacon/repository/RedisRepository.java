package com.serli.oracle.of.bacon.repository;

import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;

public class RedisRepository {
	// Constants for Redit Server
	private String serverConnection = "localhost";
	private String searchesTableName = "lastSearches";
	private int searchesNumber = 10;
	
	// Redis client
	Jedis jedis = null;
	
	public RedisRepository() {
    	// Trying to connect the redis server
    	connect();
	}
	
    public List<String> getLastTenSearches() {
        // TODO implement last 10 searches
    	
    	List<String> lastTenSearches = new ArrayList<String>();
    	
        if (jedis != null) {
        	// Connection successful
        	lastTenSearches = jedis.lrange(searchesTableName, 0, searchesNumber-1);
        }
        
        return lastTenSearches;
    }
    
    public Jedis connect() {
    	// Connecting to Redis server
        jedis = new Jedis(serverConnection);
        System.out.println("Connection to server sucessfully");
        
        // Check whether server is running or not
        if (jedis.ping().equals("PONG"))
        	return jedis;
        
        return null;
    }
    
    public boolean addNewSearch(String search) {
        if (jedis != null) {
	    	if (jedis.lpush(searchesTableName, search) < 0) {
	    		// Insertion of last search failed
	    		return false;	
	    	}	    	
        } else {
        	// Client not connected to the server
        	return false;
        }
        
    	return true;
    }
}
