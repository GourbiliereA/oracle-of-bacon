package com.serli.oracle.of.bacon.repository;

import org.junit.Test;

import junit.framework.TestCase;
import redis.clients.jedis.Jedis;

public class RedisRepositoryTest extends TestCase {
	private RedisRepository redisRepo = new RedisRepository();
	
	@Test
	public void testConnect() {
		Jedis jedis = redisRepo.connect();
		assertNotNull(jedis);
		
		String response = jedis.ping();
		assertEquals("PONG", response);
	}
	
	@Test
	public void testGetLastTenSearches() {
		redisRepo.getLastTenSearches();
	}
	
	@Test
	public void testAddNewSearch() {
		assertTrue(redisRepo.addNewSearch("Peckinpah, Sam"));
	}
}
