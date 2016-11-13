package org.researchgraph.link.nodes;

public class LinkerConfiguration {
	private String type;
	private String property;
	private String[] labels;
	
	public LinkerConfiguration() {}
	public LinkerConfiguration(String type, String property, String[] labels) {
		this.type = type;
		this.property = property;
		this.labels = labels;
	}
	
	public String getType() {
		return type;
	}
	
	public String getProperty() {
		return property;
	}
	
	public String[] getLabels() {
		return labels;
	}
	
	public void setType(String type) {
		this.type = type;
	}
	
	public void setProperty(String property) {
		this.property = property;
	}
	
	public void setLabels(String[] labels) {
		this.labels = labels;
	}

}
