package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.internal.InternalPath;
import org.neo4j.driver.internal.value.PathValue;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.ArrayList;
import java.util.List;


public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"));
    }

    public List<GraphData> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        
        // Request database
        StatementResult result = session.run("MATCH path=shortestpath((bacon:Actor)-[*0..]-(xx:Actor)) "
        		+ "WHERE bacon.name = 'Bacon, Kevin (I)' AND xx.name =~ {actorname} RETURN path;", 
        		parameters("actorname", actorName));
        
        List<GraphData> listData = new ArrayList<GraphData>();
        
        while (result.hasNext())
        {
            Record record = result.next();
            PathValue path = (PathValue) record.get("path");
            InternalPath ipath = (InternalPath) path.asPath();
            
            for (Node node : ipath.nodes()) {
            	GraphNode gNode = null;
            	if(node.hasLabel("Movie")) {
            		gNode = new GraphNode(node.id(), node.get("title").toString(), "Movie");
            	}
            	else if(node.hasLabel("Actor")) {
            		gNode = new GraphNode(node.id(), node.get("name").toString(), "Actor");
            	}
            	listData.add(new GraphData(gNode));
			}
            for (Relationship relationship : ipath.relationships()) {
				GraphEdge edge = new GraphEdge(relationship.id(), relationship.startNodeId(), relationship.endNodeId(), relationship.type());
				listData.add(new GraphData(edge));
            }
        }
        session.close();
        return listData;
    }
    
    private static class GraphData {
    	public final GraphItem data;
    	
    	 private GraphData(GraphItem data) {
             this.data = data;
         }
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
