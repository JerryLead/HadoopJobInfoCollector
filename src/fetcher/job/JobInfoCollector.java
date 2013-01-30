package fetcher.job;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import fetcher.log.MapperLogExtractor;
import fetcher.log.ReducerLogExtractor;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import task.MapTaskDetails;
import task.ReduceTaskDetails;
import util.DateParser;
import util.HtmlFetcher;

public class JobInfoCollector {
	private String status;
	
	private String hostname;
	private String jobId;

	
	private String mapTaskWholeUrl;
	private String reduceTaskWholeUrl;
	
	private double ratio = 100000;
	private long jobStartTime;
	private long jobFinishTime;
	
	private String cstStartTime;
	private String cstFinishTime;
	
	private int mapStageFinishTime;
	
	private int mapNumber;
	private int reduceNumber;
	
	private MapperLogExtractor mapperLogExtractor;
	private ReducerLogExtractor reducerLogExtractor;
	
	private String outputDir;
	
	private String[] mapMetrics = {
			"FILE_BYTES_READ",
			"HDFS_BYTES_READ",
			"FILE_BYTES_WRITTEN",
			//"HDFS_BYTES_WRITTEN",
			
			"Combine output records",
			"Map input records",
			"Spilled Records",
			"Map output bytes",
			"Map input bytes",
			"Combine input records",
			"Map output records"
			};
	
	private String reduceMetrics[] = {
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
	
	public JobInfoCollector(String hostname, String jobId, String dir) {
	
		this.hostname = hostname;
		this.jobId = jobId;
		this.outputDir = dir + jobId + File.separator;
		
		String jobWholeUrl = "http://" + hostname + ":50030/jobdetails.jsp?jobid=" + jobId;
		mapTaskWholeUrl = "http://" + hostname + ":50030/jobtasks.jsp?jobid=" + jobId
				+ "&type=map&pagenum=1";
		reduceTaskWholeUrl = "http://" + hostname + ":50030/jobtasks.jsp?jobid=" + jobId
				+ "&type=reduce&pagenum=1";

		Document wholeJspDoc = HtmlFetcher.getHtml(jobWholeUrl);
		Element body = wholeJspDoc.getElementsByTag("body").first();
		
		Element statusElem = body.select(":containsOwn(Status:)").first();
		status = statusElem.nextSibling().toString().trim();
		
		if(status.equals("Succeeded")) {
			Element startTimeElem = body.select(":containsOwn(Started at:)").first();
			cstStartTime = startTimeElem.nextSibling().toString();
			jobStartTime = DateParser.parseJobStartTime(cstStartTime.trim());
			//System.out.println("Started at:" + cstStartTime);
			
			Element finishTimeElem = body.select(":containsOwn(Finished at:)").first();
			cstFinishTime = finishTimeElem.nextSibling().toString();
			//System.out.println("Finished at:" + cstFinishTime);
			jobFinishTime = DateParser.parseJobFinishTime(cstFinishTime.trim());
			//System.out.println("Last Time:" + (jobFinishTime - jobStartTime)/1000);
			
			//System.out.println();
			System.out.println("Parsing " + jobId + "...");
			Element mapNumberElem = body.getElementsByTag("tbody").first().child(1);
			Element reduceNumberElem = body.getElementsByTag("tbody").first().child(2);
			
			mapNumber = Integer.parseInt(mapNumberElem.child(2).text());
			reduceNumber = Integer.parseInt(reduceNumberElem.child(2).text());
			
			mapperLogExtractor = new MapperLogExtractor(reduceNumber);
			reducerLogExtractor = new ReducerLogExtractor();
		}
		
	}
	
	public String getStatus() {
		return status;
	}
	
	public String getJobStartTime() {
		return cstStartTime;
	}
	
	public String getJobFinishTime() {
		return cstFinishTime;
	}
	
	public long getLastTime() {
		return (jobFinishTime - jobStartTime)/1000;
	}
	
	public int getMapStageFinishTime() {
		return mapStageFinishTime;
	}
	
	public List<MapTaskDetails> getMapTaskDetailsList(int outputMapTaskLogNum, File mlogFile) {

		List<MapTaskDetails> mapTaskList = new ArrayList<MapTaskDetails>();
		Document mapTaskWholeJspDoc = HtmlFetcher.getHtml(mapTaskWholeUrl);
		Elements mapTrs = mapTaskWholeJspDoc.getElementsByTag("tbody").first()
				.children();
		
		PrintWriter logWriter = null;
		if(outputMapTaskLogNum >= 0) {
			if(!mlogFile.getParentFile().exists()) {
				mlogFile.getParentFile().mkdirs();
			}
			
			try {
				logWriter = new PrintWriter(new BufferedWriter(new FileWriter(mlogFile)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		if(outputMapTaskLogNum == 0)
			outputMapTaskLogNum = mapTrs.size();
		
		for (int i = 1; i < mapTrs.size(); i++) {
			Element tr = mapTrs.get(i);
			MapTaskDetails newTask = new MapTaskDetails();
			
			//http://master:50030/taskdetails.jsp?jobid=job_201208242049_0013&tipid=task_201208242049_0013_m_000000
			newTask.setMapInfoLink(tr.child(0).child(0).absUrl("href"));
			
			//parse Mapper basic infos such as start/finish time, counters link, metrics link and so on.
			setMapTaskBasicInfo(newTask, newTask.getMapInfoLink());
			//System.out.println(newTask.getCounterLink());
			// get the counters information
			setMapTaskCounters(newTask, newTask.getCounterLink());
			
			
			if(outputMapTaskLogNum > 0) {
				//parse the log information
				setMapTaskLogInfos(newTask.getLogLink(), logWriter, newTask.getTaskId());
				outputMapTaskLogNum--;
			}		
			
			mapTaskList.add(newTask);
			// System.out.println(newTask);
		}
		
		if(logWriter != null)
			logWriter.close();
		
		return mapTaskList;

	}

	private void setMapTaskLogInfos(String logLink, PrintWriter logWriter, String taskId) {
		
		// directly write the parsed log info into "mapper-log-profile.txt"
		
		mapperLogExtractor.parseMapperLog(logLink, logWriter, taskId);
		
	}
	
	private void setMapTaskBasicInfo(MapTaskDetails newTask, String mapInfoLink) {
		
		Document mapDetails = HtmlFetcher.getHtml(mapInfoLink);
		
		Element tr = null;
		for(Element elem : mapDetails.getElementsByTag("tbody").first().children()) {
			if(elem.child(2).text().equals("SUCCEEDED")) {
				tr = elem;
				break;
			}
		}
			
		newTask.setTaskId(tr.child(0).text());
		
		String machine = tr.child(1).text();
		newTask.setMachine(machine.substring(machine.lastIndexOf('/')+1));
		
		newTask.setStartTime(tr.child(4).text());
		String finishTime = tr.child(5).text();
		newTask.setFinishTime(finishTime.split("\\(")[0].trim());
		
		//newTask.setLastTime(stringToTime(finishTime.split("\\(")[1].replace(')', ' ').trim()));
		newTask.setError(tr.child(6).text());
		newTask.setCounters(Integer.parseInt(tr.child(8).text()));
		newTask.setCounterLink(tr.child(8).child(0).absUrl("href"));
		
		newTask.setLogLink(tr.child(7).child(4).absUrl("href"));
		
		//parse input split locations
		tr = mapDetails.getElementsByTag("tbody").last();
		for(Element elem : tr.children()) {
			String location = elem.text();
			newTask.addLocations(location.substring(location.lastIndexOf('/') + 1));
		}		
	}

	public void setMapTaskCounters(MapTaskDetails newTask, String counterLink) {
		Document countersDoc = HtmlFetcher.getHtml(counterLink);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first()
				.children();
		for(Element elem : countersTrs) {
			if(elem.getElementsByTag("td").size() == 3) {
				newTask.setCounters(elem.child(1).text(), elem.child(2).text());
			}
		}
	}

	public List<ReduceTaskDetails> getReduceTaskDetailsList(int outputReduceTaskLogNum, File rlogFile) {

		List<ReduceTaskDetails> reduceTaskList = new ArrayList<ReduceTaskDetails>();
		Document reduceTaskWholeJspDoc = HtmlFetcher.getHtml(reduceTaskWholeUrl);
		Elements reduceTrs = reduceTaskWholeJspDoc.getElementsByTag("tbody").first().children();
		
		PrintWriter logWriter = null;
		if(outputReduceTaskLogNum >= 0) {
			if(!rlogFile.getParentFile().exists()) {
				rlogFile.getParentFile().mkdirs();
			}
			
			try {
				logWriter = new PrintWriter(new BufferedWriter(new FileWriter(rlogFile)));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(outputReduceTaskLogNum == 0)
			outputReduceTaskLogNum = reduceTrs.size();
		
		for (int i = 1; i < reduceTrs.size(); i++) {
			Element tr = reduceTrs.get(i);
			ReduceTaskDetails newTask = new ReduceTaskDetails();
			
			
			newTask.setReduceInfoLink(tr.child(0).child(0).absUrl("href"));
			
			//parse Reducer basic infos such as start/finish time, counters link, metrics link and so on.
			//System.out.println(newTask.getReduceInfoLink());
			setReduceTaskBasicInfo(newTask, newTask.getReduceInfoLink());
			
			// get the counters information
			setReduceTaskCounters(newTask, newTask.getCounterLink());
			
			if (outputReduceTaskLogNum > 0) {
				//parse the log information
				setReduceTaskLogInfos(newTask.getLogLink(), logWriter, newTask.getTaskId());
	
				outputReduceTaskLogNum --;
			}
			
			reduceTaskList.add(newTask);
			// System.out.println(newTask);
		}
		
		if(logWriter != null)
			logWriter.close();
		
		return reduceTaskList;

	}

	private void setReduceTaskLogInfos(String logLink, PrintWriter logWriter, String taskId) {
		// directly write the parsed log info into "mapper-log-profile.txt"
		reducerLogExtractor.parseReducerLog(logLink, logWriter, taskId);
	}
	
	//lastTime **min**sec
	public int stringToTime(String lastTime) {

		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < lastTime.length(); i++) {
			char c = lastTime.charAt(i);
			if(c >= '0' && c <= '9')
				sb.append(c);
			else 
				sb.append(' ');
		}
		String ints[] = sb.toString().split("\\s+");
		int sec = 0;
		for(int i = 0; i < ints.length; i++) {
			sec += sec * 60 + Integer.parseInt(ints[i]);
		}
		return sec;
	}
	
	private void setReduceTaskBasicInfo(ReduceTaskDetails newTask, String reduceInfoLink) {
		Document reduceDetails = HtmlFetcher.getHtml(reduceInfoLink);
		Element tr = null;
		for(Element elem : reduceDetails.getElementsByTag("tbody").first().children()) {
			if(elem.child(2).text().equals("SUCCEEDED")) {
				tr = elem;
				break;
			}
		}
		
		newTask.setTaskId(tr.child(0).text());
		String machine = tr.child(1).text();
		newTask.setMachine(machine.substring(machine.lastIndexOf('/')+1));
		
		newTask.setStartTime(tr.child(4).text());
		String shuffleFinishTime = tr.child(5).text();
		String sortFinishTime = tr.child(6).text();
		String finishTime = tr.child(7).text();
		newTask.setError(tr.child(8).text());
		
		newTask.setShuffleFinishTime(shuffleFinishTime.split("\\(")[0].trim());
		//newTask.setShuffleLastTime(stringToTime(shuffleFinishTime.split("\\(")[1].replace(')', ' ')
		//		.trim()));
	
		newTask.setSortFinishTime(sortFinishTime.split("\\(")[0].trim());
		//newTask.setSortLastTime(stringToTime(sortFinishTime.split("\\(")[1].replace(')', ' ')
		//		.trim()));
		newTask.setFinishTime(finishTime.split("\\(")[0].trim());
		
		newTask.setLogLink(tr.child(9).child(4).absUrl("href"));
		newTask.setCounters(Integer.parseInt(tr.child(10).text()));
		newTask.setCounterLink(tr.child(10).child(0).absUrl("href"));
	}

	public void setReduceTaskCounters(ReduceTaskDetails newTask, String url) {
		Document countersDoc = HtmlFetcher.getHtml(url);
		Elements countersTrs = countersDoc.getElementsByTag("tbody").first()
				.children();
		for(Element elem : countersTrs) {
			if(elem.getElementsByTag("td").size() == 3) {
				newTask.setCounters(elem.child(1).text(), elem.child(2).text());
			}
		}
	}
	
	public void printMapListInfos(List<MapTaskDetails> mapList, File mapFile, String[] mapMetrics) {
		
		String tab = "\t";		
		try {
			if(!mapFile.getParentFile().exists()) {
				mapFile.getParentFile().mkdirs();
			}
				
			PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(mapFile)));
			
			if(mapList.isEmpty())
				return;
			for(String metric : mapMetaMetrics) {
				writer.print(metric);
				writer.print(tab);
			}
			writer.print(mapList.get(0).getMapCountersMap().printKeys(mapMetrics));
			writer.println("\ttaskId");
			
			mapStageFinishTime = mapList.get(0).getFinishTime();
			
			for (int i = 0; i < mapList.size(); i++) {
				MapTaskDetails mapTask = mapList.get(i);
				writer.print(mapTask.printMetaMetrics(mapMetaMetrics, tab));
				writer.print(mapTask.getMapCountersMap().printKeyValues(tab, mapMetrics));
				writer.println("\t" + i);
				
				if(mapTask.getFinishTime() > mapStageFinishTime)
					mapStageFinishTime = mapTask.getFinishTime();
			}	
			
			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void printReduceListInfos(List<ReduceTaskDetails> reduceList, File reduceFile,
			String[] reduceMetrics) {
		
		String tab = "\t";
		 
		try {
			if(!reduceFile.getParentFile().exists()) {
				reduceFile.getParentFile().mkdirs();
			}
			PrintWriter writer = new PrintWriter(new BufferedWriter(
					new FileWriter(reduceFile)));
		
			if(reduceList.isEmpty())
				return;
			
			for(String metric : reduceMetaMetrics) {
				writer.print(metric);
				writer.print(tab);
			}
			writer.print(reduceList.get(0).getReduceCountersMap().printKeys(reduceMetrics));
			writer.println("\ttaskId");
			
			
			for (int i = 0; i < reduceList.size(); i++) {
				ReduceTaskDetails reduceTask = reduceList.get(i);
				writer.print(reduceTask.printMetaMetrics(reduceMetaMetrics, tab));
				
				writer.print(reduceTask.getReduceCountersMap().printKeyValues(tab, reduceMetrics));
				
				writer.println("\t" + i);

			}

			writer.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void displayMapTasks(File mapFile,  int outputMapTaskLogNum, File mlogFile) {
		//time/counter infos are kept in MapTaskDetails, log infos are directly written into mlogFile
		List<MapTaskDetails> mapList = getMapTaskDetailsList(outputMapTaskLogNum, mlogFile);
		//Collections.sort(mapList);
		printMapListInfos(mapList, mapFile, mapMetrics);
	}
	
	public void displayReduceTasks(File reduceFile, int outputReduceTaskLogNum, File rlogFile) {
		if(reduceNumber == 0)
			return;
		//time/counter infos are kept in MapTaskDetails, log infos are directly written into rlogFile
		List<ReduceTaskDetails> reduceList = getReduceTaskDetailsList(outputReduceTaskLogNum, rlogFile);
		//Collections.sort(reduceList);
		printReduceListInfos(reduceList, reduceFile, reduceMetrics);
	}
	
	private String[] mapMetaMetrics = {
			"lastTime",
			"machine",
			"startTime",
			"finishTime",
	};
	private String[] reduceMetaMetrics = {
			"lastTime",
			"machine",
			"startTime",
			"shuffleFinished",
			"sortFinished",
			"finishTime",
	};
	
	private static String nextJobId(String jobId2) {
		String jobIdBase = jobId2.substring(0, jobId2.lastIndexOf('_') + 1);
		int num = Integer.parseInt(jobId2.substring(jobId2.lastIndexOf('_') + 1));
		num++;
		return jobIdBase + String.format("%1$04d", num);
	}
	
	public void writeJobCountersAndHistory() {
		String jobWholeUrl = "http://" + hostname + ":50030/jobdetails.jsp?jobid=" + jobId;

		Document wholeJspDoc = HtmlFetcher.getHtml(jobWholeUrl);
		Element countersTable = wholeJspDoc.getElementsByTag("p").get(0).nextElementSibling();
		
		String historyUrl = "http://" + hostname + ":50030/jobhistory.jsp";
		Document historyDoc = HtmlFetcher.getHtml(historyUrl);
		Element historyElem = historyDoc.getElementsMatchingOwnText(jobId).first();
		String jobHistoryUrl = historyElem.absUrl("href");
		
		Document jobHistoryDoc = HtmlFetcher.getHtml(jobHistoryUrl);
		jobHistoryDoc.getElementsByTag("body").first().appendElement("hr");
		jobHistoryDoc.getElementsByTag("body").first().appendChild(countersTable);
		
		
		PrintWriter writer;
		File file = new File(outputDir + "jobCountersAndTime.html");
		
		if(!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));

			writer.println(jobHistoryDoc.html());
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		System.out.println("[JobCountersAndTime] writing finished!");
		
	}
	
	public void writeJobConfParameters() {

		String jobConfUrl = "http://" + hostname + ":50030/jobconf.jsp?jobid=" + jobId;
		
		
		
		Document wholeJspDoc = HtmlFetcher.getHtml(jobConfUrl);
		Element body = wholeJspDoc.getElementsByTag("body").first();

		Element confTable = body.getElementsByTag("tbody").first();
		
		File file = new File(outputDir + "conf.txt");
		
		if(!file.getParentFile().exists()) {
			file.getParentFile().mkdirs();
		}
		
		PrintWriter writer;
		try {
			writer = new PrintWriter(new BufferedWriter(new FileWriter(file)));

			for (Element elem : confTable.children()) {
				if (!elem.child(0).text().equals("name")) 
					writer.println(elem.child(0).text() + "\t" + elem.child(1).text());
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
		System.out.println("[Configuration] writing finished!");
	}
	
	public static void main(String[] args) {
		//collect timeline, configuration, final counters and log analysis
		String jobId = "job_201301281455_0001";
		String hostname = "master";
		int jobNumber = 192;
		
		int outputMapTaskLogNum = 0; // 0 stands for no limitation
		int outputReduceTaskLogNum = 0; // 0 stands for no limitation
		
		//String jobName = "BigTwitterBiDirectEdgeCount";
		//String jobName = "SampleTeraSort";
		//String jobName = "SampleWikiWordCount-1G";
		//String jobName = "SampleBuildInvertedIndex-1G";
		String jobName = "SampleTwitterInDegreeCount";
		//String dir = "/home/xulijie/MR-MEM/BigExperiments/BuildCompIndex-m36-r18/Tasklogs/";
		//String dir = "/home/xulijie/MR-MEM/BigExperiments/Wiki-m36-r18/Tasklogs/";
		//String dir = "/home/xulijie/MR-MEM/BigExperiments/BigTwitterInDegreeCount/Tasklogs/";
		//String dir = "/home/xulijie/MR-MEM/BigExperiments/BigTwitterGraphReverser/Tasklogs/";
		
		String dir = "/home/xulijie/MR-MEM/SampleExperiments/" + jobName + "/Tasklogs/";
		//String dir = "/home/xulijie/MR-MEM/NewExperiments/TeraSort-256MB-m36-r18/Tasklogs/";
		//String dir = "/home/xulijie/MR-MEM/NewExperiments/uservisits_aggre-pig-256MB/Tasklogs/";
		
		for(int i = 0; i < jobNumber; i++) {
			
			//System.out.println("Analyzing " + jobId + "'s logs...");
			File mapFile = new File(dir + jobId + File.separator + "mapper-time-counters.txt");
			File reduceFile = new File(dir + jobId + File.separator + "reducer-time-counters.txt");
			File mlogFile = new File(dir + jobId + File.separator + "mapper-log-profile.txt");
			File rlogFile = new File(dir + jobId + File.separator + "reducer-log-profile.txt");
			
			JobInfoCollector newJob = new JobInfoCollector(hostname, jobId, dir);
			
			if(newJob.getStatus().equals("Failed")) {
				jobId = nextJobId(jobId);
				continue;
			}
				
			
			newJob.writeJobConfParameters();
			newJob.writeJobCountersAndHistory();
			
			//write time/counters/log information
			newJob.displayMapTasks(mapFile, outputMapTaskLogNum, mlogFile);
			System.out.println("[Mapper Log] analysis finished!");
			
			newJob.displayReduceTasks(reduceFile, outputReduceTaskLogNum, rlogFile);
			System.out.println("[Reducer Log] analysis finished!");
			System.out.println();
			jobId = nextJobId(jobId);
		}
	}
}
