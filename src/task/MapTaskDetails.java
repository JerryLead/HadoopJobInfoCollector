package task;

import java.util.ArrayList;
import java.util.List;

import util.DateParser;

public class MapTaskDetails implements Comparable<MapTaskDetails> {
	//map basic infos

	private String taskId;
	
	
	private int startTime;
	private int finishTime;
	private int lastTime; //finishTime - startTime
	private String error;
	private int counters;
	private String mapInfoLink;
	private String counterLink;
	private String logLink;
	private String monitorMetricsLink;
	private String machine;
	private List<String> locations = new ArrayList<String>();
	
	private int mapProcess;
	private int reduceProcess;
	private int mapTurnIndex; //running in which turn
	private int processId; //indicate the process number
	
	private MapCounters mapCountersMap = new MapCounters();
	
	public String getTaskId() {
		return taskId;
	}
	public void setTaskId(String taskId) {
		this.taskId = taskId;
	}

	public int getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = DateParser.getSecondTime(startTime);
	}
	public int getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(String finishTime) {
		this.finishTime = DateParser.getSecondTime(finishTime);
		this.lastTime = this.finishTime - this.startTime;
	}
	public int getLastTime() {
		return lastTime;
	}

	public String getError() {
		return error;
	}
	public void setError(String error) {
		this.error = error;
	}
	public int getCounters() {
		return counters;
	}
	public void setCounters(int counters) {
		this.counters = counters;
	}
	public String getCounterLink() {
		return counterLink;
	}
	public void setCounterLink(String counterLink) {
		this.counterLink = counterLink;
	}
	public String getLogLink() {
		return logLink;
	}
	public void setLogLink(String logLink) {
		this.logLink = logLink;
	}
	
	public String getMonitorMetricsLink() {
		return monitorMetricsLink;
	}
	public void setMonitorMetricsLink(String monitorMetricsLink) {
		this.monitorMetricsLink = monitorMetricsLink;
	}
	public MapCounters getMapCountersMap() {
		return mapCountersMap;
	}
	public void setMapCountersMap(MapCounters mapCountersMap) {
		this.mapCountersMap = mapCountersMap;
	}
	
	
	public void setCounters(String counter, String value) {
		
		mapCountersMap.set(counter, Long.parseLong(value.replaceAll(",", "")));
	}
	public String toString() {
		return taskId + '\n' + startTime + '\n' + finishTime
		 + '\n' + lastTime + '\n' + error + '\n' + counters + '\n' + counterLink + '\n'
		 + mapCountersMap.toString();
	}
	public void setMapInfoLink(String mapInfoLink) {
		this.mapInfoLink = mapInfoLink;
	}
	public String getMapInfoLink() {
		return mapInfoLink;
	}
	public void setMachine(String machine) {
		this.machine = machine;
	}
	public String getMachine() {
		return machine;
	}

	public void addLocations(String locate) {
		locations.add(locate);
	}
	public List<String> getLocations() {
		return locations;
	}
	
	public String printMetaMetrics(String[] mapMetaMetrics, String tab) {
		
		StringBuilder sb = new StringBuilder("");
		for(String metric : mapMetaMetrics) {
			if(metric.equals("lastTime"))
				sb.append(lastTime);
			else if(metric.equals("machine"))
				sb.append(machine);
			else if(metric.equals("startTime"))
				sb.append(startTime);
			else if(metric.equals("finishTime"))
				sb.append(finishTime);
			sb.append(tab);
			
		}
		//sb.deleteCharAt(sb.length()-1);
		//if(tab.contains(","))
		//	sb.deleteCharAt(sb.length()-1);
		return sb.toString();
		
	}
	
	@Override
	public int compareTo(MapTaskDetails o) {
		if(machine.compareToIgnoreCase(o.machine) != 0)
			return machine.compareToIgnoreCase(o.machine);
		if(((Integer)startTime).compareTo(o.startTime) != 0)
			return ((Integer)startTime).compareTo(o.startTime);
		
		return ((Integer)finishTime).compareTo(o.finishTime);
	}
	
	public void setMetrics(String key, String value) {
		if(key.equals("lastTime")) {
			this.lastTime = Integer.parseInt(value);
		}
		else if (key.equals("machine")) {
			this.machine = value;
		}
		else if (key.equals("startTime")) {
			this.startTime = Integer.parseInt(value);
		}
		else if(key.equals("finishTime")) {
			this.finishTime = Integer.parseInt(value);
		}
		else 
			setCounters(key, value);
	}
	public void setMapProcess(int mapProcess) {
		this.mapProcess = mapProcess;
	}
	public int getMapProcess() {
		return mapProcess;
	}
	public void setReduceProcess(int reduceProcess) {
		this.reduceProcess = reduceProcess;
	}
	public int getReduceProcess() {
		return reduceProcess;
	}
	public void setMapTurnIndex(int mapTurnIndex) {
		this.mapTurnIndex = mapTurnIndex;
	}
	public int getMapTurnIndex() {
		return mapTurnIndex;
	}
	public void setProcessId(int processId) {
		this.processId = processId;
	}
	public int getProcessId() {
		return processId;
	}

}
