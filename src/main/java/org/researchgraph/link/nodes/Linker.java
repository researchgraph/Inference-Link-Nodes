package org.researchgraph.link.nodes;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.researchgraph.graph.GraphUtils;

public class Linker {
	private static final String NEO4J_CONF = "/conf/neo4j.conf";
	private static final String NEO4J_DB = "/data/databases/graph.db";
	
	private GraphDatabaseService graphDb;
	
	private RelationshipType relKnownAs = RelationshipType.withName( GraphUtils.RELATIONSHIP_KNOWN_AS );
	
	private boolean verbose = false;
	//private long processed = 0;
	//private long linked = 0;
//	private long skyped = 0;
	
	public Linker(final String neo4jFolder) throws Exception {
		graphDb = getGraphDb( neo4jFolder );
	}
	
	public boolean isVerbose() {
		return verbose;
	}

	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	private static File GetDbPath(final String folder) throws Exception, IOException
	{
		File db = new File(folder, NEO4J_DB);
		if (!db.exists())
			db.mkdirs();
				
		if (!db.isDirectory())
			throw new Exception("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return db;
	}
	
	private static File GetConfPath(final String folder) throws Exception
	{
		File conf = new File(folder, NEO4J_CONF);
		if (!conf.exists() || conf.isDirectory())
			throw new Exception("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return conf;
	}	
	
	private static GraphDatabaseService getGraphDb( final String graphDbPath ) throws Exception {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Exception("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.newGraphDatabase();
		
			registerShutdownHook( graphDb );
		
			return graphDb;
		} catch (Exception e) {
			throw new Exception("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    });
	}
	
	private static Long countNodes(GraphDatabaseService graphDb, String type, String source) {
		try ( Transaction ignore = graphDb.beginTx() ) {
			try (Result result = graphDb.execute("MATCH (n:" + type  + ":" + source + ") RETURN COUNT(n)")) {
				if (result.hasNext()) {
					Map<String, Object> row = result.next();
					return (Long) row.get("COUNT(n)");
				}
			}	
		}
		
		return null;
	}
	
	private static IndexDefinition createIndex(Schema schema, Label label, String key) {
		for (IndexDefinition index : schema.getIndexes(label))
			for (String property : index.getPropertyKeys())
				if (property.equals(key))
					return index;  // already existing
			
		return schema
				.indexFor(label)
				.on(key)
				.create();
	}
	
	private static Relationship findRelationship(Iterable<Relationship> rels, long nodeId, Direction direction) {
		for (Relationship rel : rels) {
			switch (direction) {
            case INCOMING:
            	if (rel.getStartNode().getId() == nodeId)
            		return rel;
            	break;
            case OUTGOING:
            	if (rel.getEndNode().getId() == nodeId)
            		return rel;
            	break;
            case BOTH:
                if (rel.getStartNode().getId() == nodeId || 
                	rel.getEndNode().getId() == nodeId)
                    	return rel;
            }
		}

        return null;
    }
	
	private static void setProperties(Relationship relationship, Map<String, Object> properties) {
		for (Map.Entry<String, Object> entry : properties.entrySet())
			relationship.setProperty(entry.getKey(), entry.getValue());
    }

	private static Relationship findRelationship(Node nodeStart, long nodeId, 
			RelationshipType type, Direction direction) {
		return findRelationship(nodeStart.getRelationships(type, direction), nodeId, direction);
	}

    private static Relationship findRelationship(Node nodeStart, Node endNode, 
    		RelationshipType type, Direction direction) {
        return findRelationship(nodeStart, endNode.getId(), type, direction);
    }
    
    private static Relationship createRelationship(Node nodeStart, Node nodeEnd, RelationshipType type) {
    	return nodeStart.createRelationshipTo(nodeEnd, type);
    }

    private static Relationship createRelationship(Node nodeStart, Node nodeEnd, RelationshipType type, 
    		Map<String, Object> properties) {
    	Relationship relationship = createRelationship(nodeStart, nodeEnd, type);
        if (null != properties)
        	setProperties(relationship, properties);

        return relationship;
    }  
	
	private static Relationship createUniqueRelationship(Node nodeStart, Node nodeEnd, RelationshipType type, 
            Direction direction, Map<String, Object> properties) {
		Relationship relationship = findRelationship(nodeStart, nodeEnd, type, direction);
		if (null == relationship)
            return createRelationship(nodeStart, nodeEnd, type, properties);

		return relationship;
	}
	

	public void link(List<LinkerConfiguration> configurations) throws Exception {
		// grants
		
		for (LinkerConfiguration cfg : configurations) {
			String type = cfg.getType();
			String property = cfg.getProperty();
			List<LinkerSource> sources = Arrays.stream(cfg.getLabels())
					.map(l -> new LinkerSource(l, countNodes(graphDb, type, l)))
					.filter(LinkerSource::notEmpty)
					.sorted()
					.collect(Collectors.toList());
			
			for (int i = 0; i < sources.size() -1; ++i) {
				for (int j = i + 1; j < sources.size(); ++j) {
					linkNodes(type, sources.get(i).getLabel(), property, sources.get(j).getLabel(), property);
				}
			}
					
		}
		
		
	/*	linkNodes(GraphUtils.TYPE_GRANT, 
				GraphUtils.SOURCE_ARC, GraphUtils.PROPERTY_ARC_ID, 
				GraphUtils.SOURCE_ANDS, GraphUtils.PROPERTY_ARC_ID);
		linkNodes(GraphUtils.TYPE_GRANT, 
				GraphUtils.SOURCE_NHMRC, GraphUtils.PROPERTY_NHMRC_ID, 
				GraphUtils.SOURCE_ANDS, GraphUtils.PROPERTY_NHMRC_ID);

		// publication
		linkNodes(GraphUtils.TYPE_PUBLICATION, GraphUtils.PROPERTY_DOI, new String[] { 
				GraphUtils.SOURCE_CROSSREF, // 10453
				GraphUtils.SOURCE_DARA,     // 57056
				GraphUtils.SOURCE_CERN,     // 174648
				GraphUtils.SOURCE_DLI,      // 306022
				GraphUtils.SOURCE_ORCID }); // 4618279
		
		// researcher
		linkNodes(GraphUtils.TYPE_RESEARCHER, GraphUtils.PROPERTY_ORCID_ID, new String[] { 
				GraphUtils.SOURCE_CROSSREF, 
				GraphUtils.SOURCE_ANDS,
				GraphUtils.SOURCE_ORCID });
					
		// Link Datasets
		linkNodes(GraphUtils.TYPE_DATASET, GraphUtils.PROPERTY_DOI, new String[] { 
				GraphUtils.SOURCE_DARA,   // 7842
				GraphUtils.SOURCE_ANDS,   // 43701
				GraphUtils.SOURCE_DRYAD,  // 57474
				GraphUtils.SOURCE_DLI }); // 1199379*/
	}
	
	private void linkNodes(String type, String property, String[] sources) {
		for (int i = 0; i < sources.length -1; ++i) {
			for (int j = i + 1; j < sources.length; ++j) {
				linkNodes(type, sources[i], property, sources[j], property);
			}
		}
 	}
	

	
	private void linkNodes(String type, 
			String srcLabel, String srcProperty, String dstLabel, String dstProperty) {
		System.out.println(String.format("Linkng %s:%s ( %s ) with %s:%s ( %s )", 
				type, srcLabel, srcProperty, type, dstLabel, dstProperty));
		
		Label labelType = Label.label(type);
		Label labelSrc = Label.label(srcLabel);
		Label labelDst = Label.label(dstLabel);
		
		try ( Transaction tx = graphDb.beginTx() ) {
			
			Schema schema = graphDb.schema();
			
			createIndex(schema, labelSrc, srcProperty);
			createIndex(schema, labelDst, dstLabel);
			
			tx.success();
		}
		
		long processed = 0;
		long linked = 0;
		
		try ( Transaction tx = graphDb.beginTx() ) {
		
			//Index<Node> index = graphDb.index().forNodes(dstLabel);
			try (ResourceIterator<Node> nodes = graphDb.findNodes( labelSrc )) {
				while (nodes.hasNext()) {
					Node node = nodes.next();
					if (node.hasLabel(labelType) && node.hasProperty(srcProperty)) {
						long id = node.getId();
						Object value = node.getProperty( srcProperty );
					
						if (verbose)
							System.out.println("Processing node: " + id);
						
						++processed;
						
						try (ResourceIterator<Node> hits = graphDb.findNodes(labelDst, dstProperty, value)) {
							if (hits.hasNext()) {
								Node hit = hits.next();
								if (hit.hasLabel(labelType)) {
									if (verbose)
										System.out.println("Establish a link with node: " + hit.getId());
									
									createUniqueRelationship(node, hit, relKnownAs, Direction.BOTH, null);
									
									++linked;
								}
							}
						}
					}
				}
			}
			
			tx.success();
		}
		
		System.out.println(String.format("Processed %d nodes", processed));
		System.out.println(String.format("Linked %d nodes", linked));
	}
	
/*	public void printStatistics(PrintStream out) {
		out.println(String.format("Totally Processed %d nodes", processed));
		out.println(String.format("Totally Linked %d nodes", linked));
	}*/	
}
