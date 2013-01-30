package task;

import java.util.HashMap;
import java.util.Map;


public class MapCounters {
	Map<String, Long> counterMap = new HashMap<String, Long>();
	String counterNames[] = {
							"FILE_BYTES_READ",
							"HDFS_BYTES_READ",
							"FILE_BYTES_WRITTEN",
							"HDFS_BYTES_WRITTEN",
							"Combine output records",
							"Map input records",
							"Spilled Records",
							"Map output bytes",
							"Map input bytes",
							"Combine input records",
							"Map output records"
							};

	public MapCounters() {
		for(String s : counterNames)
			counterMap.put(s, (long) -1);
	}
	
	public Long getFILE_BYTES_READ() {
		return counterMap.get("FILE_BYTES_READ");
	}
	
	public Long getHDFS_BYTES_READ() {
		return counterMap.get("HDFS_BYTES_READ");
	}
	public Long getFILE_BYTES_WRITTEN() {
		return counterMap.get("FILE_BYTES_WRITTEN");
	}
	public Long getHDFS_BYTES_WRITTEN() {
		return counterMap.get("HDFS_BYTES_WRITTEN");
	}
	public Long getCombine_output_records() {
		return counterMap.get("Combine output records");
	}
	public Long getMap_input_records() {
		return counterMap.get("Map input records");
	}
	public Long getSpilled_Records() {
		return counterMap.get("Spilled Records");
	}
	public Long getMap_output_bytes() {
		return counterMap.get("Map output bytes");
	}
	public Long getMap_input_bytes() {
		return counterMap.get("Map input bytes");
	}
	public Long getCombine_input_records() {
		return counterMap.get("Combine input records");
	}
	public Long getMap_output_records() {
		return counterMap.get("Map output records");
	}
	
	public void set(String counter, Long value) {
		if(!counterMap.containsKey(counter))
			System.err.println("counterMap doesn't contain " + counter);
		else
			counterMap.put(counter, value);
	}
	
	public String printKeys(String[] mapMetrics) {
		StringBuilder sb = new StringBuilder("");
		/*
		for(Entry<String, Long> entry : counterMap.entrySet()) {
			if(entry.getValue() != -1) {
				sb.append(entry.getKey());
				sb.append("\t");
			}	
		}
		*/
		for(String metric : mapMetrics) {
			if(counterMap.containsKey(metric)) {
				sb.append(metric);
				sb.append("\t");
			}
		}
		sb.deleteCharAt(sb.length()-1);
		return sb.toString();
	}
	
	public String printKeyValues(String tab, String[] mapMetrics) {
		StringBuilder sb = new StringBuilder("");
		/*
		for(Entry<String, Long> entry : counterMap.entrySet()) {
			if(entry.getValue() != -1) {
				sb.append(entry.getValue());
				sb.append(tab);
			}
		}
		*/
		for(String metric : mapMetrics) {
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
