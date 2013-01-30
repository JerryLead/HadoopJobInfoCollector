package task;

import util.DateParser;

public class ReduceTaskDetails implements Comparable<ReduceTaskDetails> {
	
	private String taskId;

	private int startTime;
	private int shuffleFinishTime;
	private int sortFinishTime;
	private int finishTime;
	private int lastTime;
	
	private String error;
	private int counters;
	private String counterLink;
	private String reduceInfoLink;
	private String machine;
	private String logLink;

	private ReduceCounters reduceCountersMap = new ReduceCounters();

	
	public String toString() {
		return taskId + '\n' + startTime + '\n' + finishTime
		 + '\n' + lastTime + '\n' + error + '\n' + counters + '\n' + counterLink + '\n'
		 + reduceCountersMap.toString() + '\n';
	}
	
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
	
	public ReduceCounters getReduceCountersMap() {
		return reduceCountersMap;
	}
	
	public void setReduceCountersMap(ReduceCounters reduceCountersMap) {
		this.reduceCountersMap = reduceCountersMap;
	}

	public void setCounters(String counter, String value) {
		reduceCountersMap.set(counter, Long.parseLong(value.replaceAll(",", "")));
	}
	
	public void setReduceInfoLink(String reduceInfoLink) {
		this.reduceInfoLink = reduceInfoLink;
	}

	public String getReduceInfoLink() {
		return reduceInfoLink;
	}

	public void setMachine(String machine) {
		this.machine = machine;
	}

	public String getMachine() {
		return machine;
	}

	public void setShuffleFinishTime(String shuffleFinishTime) {
		this.shuffleFinishTime = DateParser.getSecondTime(shuffleFinishTime);
	}

	public int getShuffleFinishTime() {
		return shuffleFinishTime;
	}

	public void setSortFinishTime(String sortFinishTime) {
		this.sortFinishTime = DateParser.getSecondTime(sortFinishTime);
	}

	public int getSortFinishTime() {
		return sortFinishTime;
	}

	public String getLogLink() {
		return logLink;
	}

	public void setLogLink(String logLink) {
		this.logLink = logLink;
	}


	public String printMetaMetrics(String[] reduceMetaMetrics, String tab) {

		StringBuilder sb = new StringBuilder("");
		for(String metric : reduceMetaMetrics) {
	
			if(metric.equals("lastTime"))
				sb.append(lastTime);
			else if(metric.equals("machine"))
				sb.append(machine);
			else if(metric.equals("startTime"))
				sb.append(startTime);
			else if(metric.equals("finishTime"))
				sb.append(finishTime);
			else if(metric.equals("shuffleFinished")) 
				sb.append(shuffleFinishTime);		
			else if(metric.equals("sortFinished")) 
				sb.append(sortFinishTime);
			
			sb.append(tab);
			
		}
		//sb.deleteCharAt(sb.length()-1);
		//if(tab.contains(","))
		//	sb.deleteCharAt(sb.length()-1);
		return sb.toString();
		
	}

	@Override
	public int compareTo(ReduceTaskDetails o) {
		if(machine.compareToIgnoreCase(o.machine) != 0)
			return machine.compareToIgnoreCase(o.machine);
		if(((Integer)startTime).compareTo(o.startTime) != 0)
			return ((Integer)startTime).compareTo(o.startTime);
		if(((Integer)sortFinishTime).compareTo(o.sortFinishTime) != 0)
			return ((Integer)sortFinishTime).compareTo(o.sortFinishTime);
		if(((Integer)finishTime).compareTo(o.finishTime) != 0)
			return ((Integer)finishTime).compareTo(o.finishTime);
		return 0;
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
		else if (key.equals("shuffleFinished"))
			this.shuffleFinishTime = Integer.parseInt(value);
		else if (key.equals("sortFinished"))
			this.sortFinishTime = Integer.parseInt(value);
		else if(key.equals("finishTime")) {
			this.finishTime = Integer.parseInt(value);
		}
		
		else 
			setCounters(key, value);
		
	}

}
