package fetcher.log;

import java.io.PrintWriter;
import java.util.Scanner;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import util.DateParser;
import util.HtmlFetcher;

public class ReducerLogExtractor {

	public void parseReducerLog(String logLink, PrintWriter logWriter, String taskId) {
	
		Document mapLogs = HtmlFetcher.getHtml(logLink);
		Element syslogPre = mapLogs.getElementsByTag("pre").last();
		String syslog[] = syslogPre.text().split("\\n");
		
		int i;
		logWriter.println("************************" + taskId + "************************");
		
		for(i = 0; i < syslog.length; i++) {
			if(syslog[i].contains("ShuffleRamManager:")) { //ShuffleRamManager: MemoryLimit=652482944, MaxSingleShuffleLimit=163120736
				long reducerStartTime = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				int start = syslog[i].indexOf('=') + 1; //652482944, MaxSingleShuffleLimit=163120736
				int end = syslog[i].indexOf(',', start);//, MaxSingleShuffleLimit=163120736
				long memoryLimit = Long.parseLong(syslog[i].substring(start, end));
				
				start = syslog[i].indexOf('=', end) + 1; //163120736
				long maxSingleShuffleLimit = Long.parseLong(syslog[i].substring(start));
				
				//reducer.setReducerStartTime(reducerStartTime);
				//reducer.getReducerBuffer().set(memoryLimit, maxSingleShuffleLimit);
				logWriter.println("[" + reducerStartTime + "] MemoryLimit = " + memoryLimit + ", MaxSingleShuffleLimit = " + maxSingleShuffleLimit); 
				i++;
				break;
			}
		}
		
		for(; i < syslog.length; i++) {
			//2012-10-13 11:57:54,795 INFO org.apache.hadoop.mapred.ReduceTask: Shuffling 34476716 bytes (5047132 raw bytes) into RAM from attempt_201210131136_0002_m_000000_0
			if(syslog[i].contains("Shuffling")) {
				int start = syslog[i].indexOf("Shuffling") + 10; 
				int end = syslog[i].indexOf(' ', start); 
				long decompressedLen = Long.parseLong(syslog[i].substring(start, end)); //34476716
				
				start = syslog[i].indexOf('(', end) + 1; 
				end = syslog[i].indexOf(' ', start); 
				long compressedLen = Long.parseLong(syslog[i].substring(start, end)); //5047132
				
				start = syslog[i].indexOf("into", end) + 5; 
				end = syslog[i].indexOf(' ', start); 
				String storeLoc = syslog[i].substring(start, end); //RAM
				
				start = syslog[i].indexOf("from", end) + 5; 
				String sourceTaskId = syslog[i].substring(start); //attempt_201208242049_0014_m_000050_0
				
				long shuffleFinishTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				logWriter.println("[Shuffling][" + shuffleFinishTimeMS + "][" + storeLoc + "] decompressedLen = " + decompressedLen + ", compressedLen = " + compressedLen);
				//reducer.getShuffle().addShuffleItem(shuffleFinishTimeMS, sourceTaskId, storeLoc, decompressedLen, compressedLen);
			}
			
			/*
			 *  LOG.info(reduceTask.getTaskID() + "We have  " + 
			 *  mapOutputFilesOnDisk.size() + " map outputs on disk. " +
			 *  "Triggering merge of " + ioSortFactor + " files");
			 */
			else if(syslog[i].contains("Triggering merge of")) {
				System.err.println("[Note] OnDiskMergeInShuffle is triggered!");
				long startMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));				
				String mergeLoc = "OnDiskShuffleMerge";
				//reducer.getMergeInShuffle().addMergeInShuffleBeforeItem(mergeLoc, startMergeTimeMS);	
				
				for(int j = i + 1; j < syslog.length; j++) {
					if(syslog[j].contains("intermediate segments")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int mergeSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Merging") + 8, syslog[j].indexOf("intermediate") - 1));
						int totalSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].lastIndexOf("of") + 3, syslog[j].lastIndexOf('<') -1));
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueString, 3);
						
						long writeRecords = values[0];
						long rawLengthInter = values[1];
						long compressedLengthInter = values[2];
						
						//reducer.getMergeInShuffle().addShuffleIntermediateMergeItem(mergeLoc, mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords,
						//		rawLengthInter, compressedLengthInter);
						logWriter.println("[Intermediate][" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
								+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
								+ ", CompressedLength = " + compressedLengthInter); 
					}
					else if(syslog[j].contains("Down to the last merge-pass")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						String valueString = syslog[j].substring(syslog[j].indexOf("Down"));
						long[] values = extractLongNumber(valueString, 2);
						
						int lastMergePassSegmentsNum = (int)values[0];
						long lastMergePassTotalSize = values[1];
						
						//reducer.getMergeInShuffle().addLastPassMergeInShuffleItem(mergeLoc, mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
						logWriter.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
								+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
					}
					
					/*
					 * LOG.info("[OnDiskShuffleMerge]<SegmentsNum = " + mapFiles.size() + ", "
					 * "Records = " +  totalRecordsBeforeCombine + ", "
					 * "BytesBeforeMerge = " + approxOutputSize + ", "
					 * "RawLength = " + writer.getRawLength() + ", "
					 * "CompressedLength = " + writer.getCompressedLength() + ">");
					 */
					else if(syslog[j].contains("[OnDiskShuffleMerge]")) {
						long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));	
						String valueStr = syslog[j].substring(syslog[j].indexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueStr, 6);
						
						int SegmentsNum = (int)values[0];
						long Records = values[1];
						long BytesBeforeMergeAC = values[2];
						long RawLength = values[3];
						long CompressedLength = values[4];
				
						//reducer.getMergeInShuffle().addShuffleAfterMergeItem(mergeLoc, stopMergeTimeMS, SegmentsNum, Records,
						//		BytesBeforeMerge, Records, RawLength, CompressedLength);
						logWriter.println("[InMemoryShuffleMerge][" + stopMergeTimeMS + "] SegmentsNum = " + SegmentsNum + ", RecordsBeforeMergeAC = " + Records
								+ ", BytesBeforeMergeAC = " + BytesBeforeMergeAC + ", RecordsAfterCombine = " + Records
								+ ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
						break;
					}
				}	
			}
			/*
			 * 2012-10-13 11:58:01,655 INFO org.apache.hadoop.mapred.ReduceTask: Initiating in-memory merge with 29 segments...
			 * 2012-10-13 11:58:01,657 INFO org.apache.hadoop.mapred.Merger: Merging 29 sorted segments
			 * 2012-10-13 11:58:01,657 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 29 segments left of total size: 998484484 bytes
			 * other lines
			 * 2012-10-13 11:58:42,240 INFO org.apache.hadoop.mapred.ReduceTask: [InMemoryShuffleMerge]<SegmentsNum = 29, RecordsBeforeMergeAC = 9789063, 
			 * BytesBeforeMergeAC = 998484484, RecordsAfterCombine = 9789063, RawLength = 998484428, CompressedLength = 149009950>
			 */
			else if(syslog[i].contains("Initiating in-memory merge")) {
				long startMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));				
				String mergeLoc = "InMemoryShuffleMerge";
				//reducer.getMergeInShuffle().addMergeInShuffleBeforeItem(mergeLoc, startMergeTimeMS);
				
				for(int j = i + 1; j < syslog.length; j++) {
					if(syslog[j].contains("intermediate segments")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int mergeSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Merging") + 8, syslog[j].indexOf("intermediate") - 1));
						int totalSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].lastIndexOf("of") + 3, syslog[j].lastIndexOf('<') -1));
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueString, 3);
						
						long writeRecords = values[0];
						long rawLengthInter = values[1];
						long compressedLengthInter = values[2];
						
						//reducer.getMergeInShuffle().addShuffleIntermediateMergeItem(mergeLoc, mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords,
						//		rawLengthInter, compressedLengthInter);
						logWriter.println("[Intermediate][" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
								+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
								+ ", CompressedLength = " + compressedLengthInter); 
					}
					else if(syslog[j].contains("Down to the last merge-pass")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						String valueString = syslog[j].substring(syslog[j].indexOf("Down"));
						long[] values = extractLongNumber(valueString, 2);
						
						int lastMergePassSegmentsNum = (int)values[0];
						long lastMergePassTotalSize = values[1];
						
						//reducer.getMergeInShuffle().addLastPassMergeInShuffleItem(mergeLoc, mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
						logWriter.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
								+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
					}
					else if(syslog[j].contains("[InMemoryShuffleMerge]")) {
						long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));	
						String valueStr = syslog[j].substring(syslog[j].indexOf('<') + 1, syslog[j].lastIndexOf('>'));
						long[] values = extractLongNumber(valueStr, 6);
						
						int SegmentsNum = (int)values[0]; //29
						long RecordsBeforeMergeAC = values[1];//9789063
						long BytesBeforeMergeAC = values[2]; //998484484
						long RecordsAfterCombine = values[3]; //9789063
						long RawLength = values[4]; //998484428
						long CompressedLength = values[5]; //149009950
				
						//reducer.getMergeInShuffle().addShuffleAfterMergeItem(mergeLoc, stopMergeTimeMS, SegmentsNum, RecordsBeforeMergeAC,
						//		BytesBeforeMergeAC, RecordsAfterCombine, RawLength, CompressedLength);
						logWriter.println("[InMemoryShuffleMerge][" + stopMergeTimeMS + "] SegmentsNum = " + SegmentsNum + ", RecordsBeforeMergeAC = " + RecordsBeforeMergeAC
								+ ", BytesBeforeMergeAC = " + BytesBeforeMergeAC + ", RecordsAfterCombine = " + RecordsAfterCombine
								+ ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
						break;
					}
				}		
			}
			//copy phase ends, there are some files left on disk and some segments in memory
			//2012-10-13 12:09:20,631 INFO org.apache.hadoop.mapred.ReduceTask: Interleaved on-disk merge complete: 12 files left.
			else if(syslog[i].contains("Interleaved on-disk merge complete")) {
				int mapOutputFilesOnDisk = Integer.parseInt(syslog[i].substring(
						syslog[i].lastIndexOf(':') + 2, syslog[i].lastIndexOf("files") - 1));
				logWriter.println("[on-disk merge complete] " + mapOutputFilesOnDisk + " files left");
				//reducer.getSort().setOnDiskSegmentsLeftAfterShuffle(mapOutputFilesOnDisk);
			}
			//copy phase ends, there are some files left on disk and some segments in memory
			//2012-10-13 12:09:49,451 INFO org.apache.hadoop.mapred.ReduceTask: In-memory merge complete: 3 files left.
			else if(syslog[i].contains("In-memory merge complete")) {
				long shufflePhaseFinishTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));	
				int mapOutputsFilesInMemory = Integer.parseInt(syslog[i].substring(
						syslog[i].lastIndexOf(':') + 2, syslog[i].lastIndexOf("files") - 1));
				logWriter.println("[Shuffle Phase finished][" + mapOutputsFilesInMemory + "]" + " files left");
				//reducer.setShufflePhaseFinishTimeMS(shufflePhaseFinishTimeMS);
				//reducer.getSort().setInMemorySegmentsLeftAfterShuffle(mapOutputsFilesInMemory);
				
				i++;
				break;
			}
		}

		//sort phase begins		
		/*
		 * [InMemorySortMerge]
		 * 2012-10-13 11:42:18,676 INFO org.apache.hadoop.mapred.ReduceTask: In-memory merge complete: 38 files left.
		 * 2012-10-13 11:42:18,686 INFO org.apache.hadoop.mapred.Merger: Merging 38 sorted segments
		 * 2012-10-13 11:42:18,686 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 38 segments left of total size: 82317689 bytes
		 * 2012-10-13 11:42:21,960 INFO org.apache.hadoop.mapred.ReduceTask: [InMemorySortMerge]<SegmentsNum = 38, Records = 5083386, BytesBeforeMerge = 82317689,
 		 * RawLength = 82317615, CompressedLength = 82317619>
 		 * 
 		 * or
 		 * 
 		 * 2012-10-13 12:09:49,451 INFO org.apache.hadoop.mapred.ReduceTask: In-memory merge complete: 3 files left.
 		 * 2012-10-13 12:09:49,453 INFO org.apache.hadoop.mapred.ReduceTask: Keeping 3 segments, 103346202 bytes in memory for intermediate, on-disk merge
		 */
		
		for(; i < syslog.length; i++) {
			//2012-10-13 12:09:49,453 INFO org.apache.hadoop.mapred.ReduceTask: Keeping 3 segments, 103346202 bytes in memory for intermediate, on-disk merge
			if(syslog[i].contains("ReduceTask: Keeping")) {
				long stopInMemorySortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int mergeSegmentsNum = Integer.parseInt(syslog[i].substring(
						syslog[i].indexOf("Keeping") + 8, syslog[i].indexOf("segments") - 1));
				long sizeAfterMerge = Long.parseLong(syslog[i].substring(
						syslog[i].indexOf("segments,") + 10, syslog[i].lastIndexOf("bytes") - 1));
				logWriter.println("[Keeping in memory][" + stopInMemorySortMergeTimeMS + "] SegmentsNum = " 
						+ mergeSegmentsNum + ", sizeAfterMerge = " + sizeAfterMerge);
				
				//reducer.getSort().setInMemorySortMergeItem(stopInMemorySortMergeTimeMS, mergeSegmentsNum, 0, sizeAfterMerge,
				//		sizeAfterMerge, sizeAfterMerge);
				i++;
				break;
			}
			//2012-10-13 11:42:21,960 INFO org.apache.hadoop.mapred.ReduceTask: [InMemorySortMerge]<SegmentsNum = 38, Records = 5083386, BytesBeforeMerge = 82317689,
	 		// RawLength = 82317615, CompressedLength = 82317619>
			else if (syslog[i].contains("[InMemorySortMerge]")) {
				long stopInMemorySortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				String valueStr = syslog[i].substring(syslog[i].indexOf('<') + 1, syslog[i].lastIndexOf('>'));
				
				long[] values = extractLongNumber(valueStr, 5);
				int SegmentsNum = (int)values[0]; //38
				long Records = values[1]; //5083386
				long BytesBeforeMerge = values[2]; //82317689
				long RawLength = values[3]; //82317615
				long CompressedLength = values[4]; //82317619
				
				logWriter.println("[InMemorySortMerge][" + stopInMemorySortMergeTimeMS + "] SegmentsNum = " + SegmentsNum
						+ ", Records = " + Records + ", BytesBeforeMerge = " + BytesBeforeMerge
						+ ", RawLength = " + RawLength 
						+ ", CompressedLength = " + CompressedLength);
				
				//reducer.getSort().setInMemorySortMergeItem(stopInMemorySortMergeTimeMS, SegmentsNum, Records, BytesBeforeMerge,
				//		RawLength, CompressedLength);
				i++;
				break;
			}
		}
		/*
		 * 2012-10-13 11:42:21,961 INFO org.apache.hadoop.mapred.Merger: Merging 1 sorted segments
		 * 2012-10-13 11:42:21,963 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 1 segments left of total size: 82317615 bytes
		 * 2012-10-13 11:42:21,963 INFO org.apache.hadoop.mapred.ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 0, InMemorySegmentsSize = 0, 
		 * OnDiskSegmentsNum = 1, OnDiskSegmentsSize = 82317619
		 * 
		 * or
		 * 
		 * 2012-10-13 12:09:49,458 INFO org.apache.hadoop.mapred.Merger: Merging 16 sorted segments
		 * 2012-10-13 12:12:12,869 INFO org.apache.hadoop.mapred.Merger: Merging 7 intermediate segments out of a total of 16 <WriteRecords = 38827533, RawLength = 3960408368, CompressedLength = 591102891>
		 * 2012-10-13 12:12:12,915 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 10 segments left of total size: 1913030519 bytes
		 * 2012-10-13 12:12:12,915 INFO org.apache.hadoop.mapred.ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 3, InMemorySegmentsSize = 103346202, 
		 * OnDiskSegmentsNum = 13, OnDiskSegmentsSize = 1896494449
		 */
		for(; i < syslog.length; i++) {
			if(syslog[i].contains("intermediate segments")) {
				long mergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int mergeSegmentsNum = Integer.parseInt(syslog[i].substring(syslog[i].indexOf("Merging") + 8, syslog[i].indexOf("intermediate") - 1));
				int totalSegmentsNum = Integer.parseInt(syslog[i].substring(syslog[i].lastIndexOf("of") + 3, syslog[i].lastIndexOf('<') -1));
				String valueString = syslog[i].substring(syslog[i].lastIndexOf('<') + 1, syslog[i].lastIndexOf('>'));
				long[] values = extractLongNumber(valueString, 3);
				
				long writeRecords = values[0];
				long rawLengthInter = values[1];
				long compressedLengthInter = values[2];
				//reducer.getSort().addIntermediateMergeInSortItem("MixSortMerge", mergeTimeMS, mergeSegmentsNum, totalSegmentsNum,
				//		writeRecords, rawLengthInter, compressedLengthInter);
				logWriter.println("[Intermediate][" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
						+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
						+ ", CompressedLength = " + compressedLengthInter); 
			}
			else if(syslog[i].contains("Down to the last merge-pass")) {
				long mergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				String valueString = syslog[i].substring(syslog[i].indexOf("Down"));
				long[] values = extractLongNumber(valueString, 2);
				
				int lastMergePassSegmentsNum = (int)values[0];
				long lastMergePassTotalSize = values[1];
				
				//reducer.getSort().addLastPassMergeInSortItem("MixSortMerge", mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
				logWriter.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
						+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
			}
			//2012-10-13 12:12:12,915 INFO org.apache.hadoop.mapred.ReduceTask: [MixSortMerge][CountersBeforeMerge]<InMemorySegmentsNum = 3, 
			//InMemorySegmentsSize = 103346202, OnDiskSegmentsNum = 13, OnDiskSegmentsSize = 1896494449
			else if (syslog[i].contains("[MixSortMerge]")) {
				long stopMixSortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				//should be
				//String valuStr = syslog[i].substring(syslog[i].indexOf('<') + 1, syslog[i].lastIndexOf('>'));
				String valueStr = syslog[i].substring(syslog[i].indexOf('<') + 1);
						
				long[] values = extractLongNumber(valueStr, 4);
				int InMemorySegmentsNum = (int)values[0]; //3 
				long InMemorySegmentsSize = values[1]; //103346202
				int OnDiskSegmentsNum = (int)values[2]; //13
				long OnDiskSegmentsSize = values[3]; //1896494449
				
				//reducer.getSort().addMixSortMergeItem(stopMixSortMergeTimeMS, InMemorySegmentsNum, InMemorySegmentsSize,
				//		OnDiskSegmentsNum, OnDiskSegmentsSize);
				//reducer.setSortPhaseFinishTimeMS(stopMixSortMergeTimeMS);
				logWriter.println("[MixSortMerge][" + stopMixSortMergeTimeMS + "] InMemorySegmentsNum = " + InMemorySegmentsNum
						+ ", InMemorySegmentsSize = " + InMemorySegmentsSize + ", OnDiskSegmentsNum = " + OnDiskSegmentsNum
						+ ", OnDiskSegmentsSize = " + OnDiskSegmentsSize);
				i++;
				break;
			}
		}
		
		for(; i < syslog.length; i++) {
			//LOG.info("[FinalSortMerge]" + "<InMemorySegmentsNum = " + inMemSegmentsNum + ", "
		  	//+ "InMemorySegmentsSize = " + inMemBytes + ">");	
			if (syslog[i].contains("[FinalSortMerge]")) {
				long stopFinalSortMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				String valueStr = syslog[i].substring(syslog[i].indexOf('<') + 1);
				long[] values = new long[2];
				
				int InMemorySegmentsNum = (int)values[0]; //inMemSegmentsNum
				long inMemBytes = values[1]; //inMemBytes		
				//reducer.getSort().addFinalSortMergeItem(stopFinalSortMergeTimeMS, InMemorySegmentsNum, inMemBytes);
				//reducer.setSortPhaseFinishTimeMS(stopFinalSortMergeTimeMS);
			}
			//2012-10-13 12:15:15,245 INFO org.apache.hadoop.mapred.TaskRunner: Task:attempt_201210131136_0002_r_000000_0 is done. And is in the process of commiting
			//2012-10-13 12:15:17,250 INFO org.apache.hadoop.mapred.TaskRunner: Task attempt_201210131136_0002_r_000000_0 is allowed to commit now
			//2012-10-13 12:15:17,278 INFO org.apache.hadoop.mapred.FileOutputCommitter: Saved output of task 'attempt_201210131136_0002_r_000000_0' to hdfs://master:9000/Output/terasort-100GB
			//2012-10-13 12:15:17,280 INFO org.apache.hadoop.mapred.TaskRunner: Task 'attempt_201210131136_0002_r_000000_0' done.
			else if (syslog[i].contains("And is in the process of commiting")) {
				long reducePhaseStopTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));	
				
				logWriter.println("reducePhaseStopTimeMS = " + reducePhaseStopTimeMS);
				//reducer.setReducePhaseStopTimeMS(reducePhaseStopTimeMS);
			}
			
			else if (syslog[i].contains("is allowed to commit now")) {
				long commitStartTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				//reducer.setCommitStartTimeMS(commitStartTimeMS);
			}
			
			else if (i == syslog.length - 1) {
				long reducerStopTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				
				//reducer.setReducerStopTimeMS(reducerStopTimeMS);
				logWriter.println("reducerStopTimeMS = " + reducerStopTimeMS);
			}
		}
		
		logWriter.println();
	}
	
	public static long[] extractLongNumber(String line, int n) {
		long[] values = new long[n];
		Scanner scanner = new Scanner(line).useDelimiter("[^0-9]+");
		int i = 0;
		
		while(scanner.hasNextLong())
			values[i++] = scanner.nextLong();
		return values;
	}
}
