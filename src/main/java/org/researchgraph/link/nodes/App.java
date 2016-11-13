package org.researchgraph.link.nodes;

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.researchgraph.configuration.Properties;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class App {
	
	public static void main(String[] args) {
		try {
			ObjectMapper mapper = new ObjectMapper();
			
			Configuration properties = Properties.fromArgs(args);
	        
	        System.out.println("Link Nodes");
	        	        
	        String neo4jFolder = properties.getString(Properties.PROPERTY_NEO4J_FOLDER);
	        System.out.println("Neo4J: " + neo4jFolder);
	        
	        List<LinkerConfiguration> configurations = mapper.readValue(new File(Properties.PROPERTY_LINK_FILE), new TypeReference<List<LinkerConfiguration>>(){});
	        
	        Linker linker = new Linker(neo4jFolder);
	        linker.link(configurations);
	      //  linker.printStatistics(System.out);
	        
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}
}
