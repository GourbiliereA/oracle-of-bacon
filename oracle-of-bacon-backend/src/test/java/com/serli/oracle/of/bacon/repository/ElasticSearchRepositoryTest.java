package com.serli.oracle.of.bacon.repository;

import java.util.List;

import org.junit.Test;

import junit.framework.TestCase;

public class ElasticSearchRepositoryTest extends TestCase {
	private ElasticSearchRepository elasticSearchRepo = new ElasticSearchRepository();
	
	private final String searchText = "niro";
	
	@Test
	public void testGetJestClient() {
		List<String> suggestions = elasticSearchRepo.getActorsSuggests(searchText);
		
		assertNotNull(suggestions);
		assertFalse(suggestions.isEmpty());
		
		System.out.println("RESULTS for \"" + searchText + "\" :");
		for (String s : suggestions) {
			System.out.println(s);
		}
	}
}
