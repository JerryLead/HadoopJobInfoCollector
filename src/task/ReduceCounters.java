package task;

import java.util.HashMap;
import java.util.Map;


public class ReduceCounters {

	Map<String, Long> counterMap = new HashMap<String, Long>();
	String counterNames[] = {
							"FILE_BYTES_READ",
							"HDFS_BYTES_WRITTEN",
							"FILE_BYTES_WRITTEN",
							"Reduce input groups",
							"Combine output records",
							"Reduce shuffle bytes",
							"Reduce output records",
							"Spilled Records",
							"Combine input records",
							"Reduce input records"
							};

	public ReduceCounters() {
		for(String s : counterNames)
			counterMap.put(s, (long) -1);
	}
	
	public Long getFILE_BYTES_READ() {
		return counterMap.get("FILE_BYTES_READ");
	}
	
	public Long getHDFS_BYTES_WRITTEN() {
		return counterMap.get("HDFS_BYTES_WRITTEN");
	}
	public Long getFILE_BYTES_WRITTEN() {
		return counterMap.get("FILE_BYTES_WRITTEN");
	}
	public Long getReduce_input_groups() {
		return counterMap.get("Reduce input groups");
	}
	public Long getCombine_output_records() {
		return counterMap.get("Combine output records");
	}
	
	public Long getSpilled_Records() {
		return counterMap.get("Spilled Records");
	}
	public Long getReduce_shuffle_bytes() {
		return counterMap.get("Reduce shuffle bytes");
	}
	public Long getReduce_output_records() {
		return counterMap.get("Reduce output records");
	}
	public Long getCombine_input_records() {
		return counterMap.get("Combine input records");
	}
	public Long getReduce_input_records() {
		return counterMap.get("Reduce input records");
	}
	
	public void set(String counter, Long value) {
		if(!counterMap.containsKey(counter))
			;//System.err.println("counterMap doesn't contain " + counter);
		else
			counterMap.put(counter, value);
	}
	public String printKeys(String[] reduceMetrics) {
		StringBuilder sb = new StringBuilder("");
		/*
		for(Entry<String, Long> entry : counterMap.entrySet()) {
			if(entry.getValue() != -1) {
				sb.append(entry.getKey());
				sb.append("\t");
			}	
		}
		*/
		for(String metric : reduceMetrics) {
			if(counterMap.containsKey(metric)) {
				sb.append(metric);
				sb.append("\t");
			}
		}
		sb.deleteCharAt(sb.length()-1);
		
		return sb.toString();
	}
	
	public String printKeyValues(String tab, String[] reduceMetrics) {
		StringBuilder sb = new StringBuilder("");
		/*
		for(Entry<String, Long> entry : counterMap.entrySet()) {
			if(entry.getValue() != -1) {
				sb.append(entry.getValue());
				sb.append(tab);
			}
		}
		*/
		for(String metric : reduceMetrics) {
			if(counterMap.containsKey(metric)) {
				sb.append(counterMap.get(metric));
				sb.append(tab);
			}
		}
		sb.deleteCharAt(sb.length()-1);
		if(tab.contains(","))
			sb.deleteCharAt(sb.length()-1);
		return sb.toString();
		
	}
	
	public String toString() {
		return counterMap.toString();
	}
}
