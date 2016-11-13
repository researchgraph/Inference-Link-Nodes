package org.researchgraph.link.nodes;

import java.util.Objects;

public class LinkerSource implements Comparable<LinkerSource> {
	private final String label;
	private final  Long count;
	
	public LinkerSource(String label, Long count) {
		this.label = label;
		this.count = count;
	}
	
	public String getLabel() {
		return label;
	}
	
	public Long getCount() {
		return count;
	}
	
	public boolean notEmpty() {
		return null != count && count > 0;
	}

	@Override
	public int compareTo(LinkerSource o) {
		return count < o.count ? -1 : count > o.count ? 1 : 0;
	}
}
