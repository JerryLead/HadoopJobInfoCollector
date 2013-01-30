package fetcher.log;

import java.io.PrintWriter;
import java.util.Scanner;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import util.DateParser;
import util.HtmlFetcher;

public class MapperLogExtractor {
	
	private int partition;
	
	public MapperLogExtractor(int reduceNumber) {
		partition = reduceNumber;
	}

	public void parseMapperLog(String logLink, PrintWriter logWriter, String taskId) {
		Document mapLogs = HtmlFetcher.getHtml(logLink);
		
		Element syslogPre = mapLogs.getElementsByTag("pre").last();
		String syslog[] = syslogPre.text().split("\\n");
		
		int i;
		logWriter.println("************************" + taskId + "************************");
		/*
		 * 2012-10-10 14:59:39,727 INFO org.apache.hadoop.mapred.MapTask: io.sort.mb = 500
		 * 2012-10-10 14:59:40,005 INFO org.apache.hadoop.mapred.MapTask: data buffer = 398458880/498073600
		 * 2012-10-10 14:59:40,005 INFO org.apache.hadoop.mapred.MapTask: record buffer = 1310720/1638400
		 */
		for(i = 0; i < syslog.length; i++) {
			if(syslog[i].contains("io.sort.mb")) {
				int ioSortMb = Integer.parseInt(syslog[i].substring(syslog[i].lastIndexOf('=') + 2)); //500
				long mapperStartTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:39
				
				logWriter.println("io.sort.mb = " + ioSortMb);
				logWriter.println("mapperStartTimeMS = " + mapperStartTimeMS);
				logWriter.println();
				//mapper.setMapperStartTimeMS(mapperStartTimeMS);
				
			}
			
			else if(syslog[i].contains("data buffer")) {
				String[] dataBuffer = syslog[i].substring(syslog[i].lastIndexOf('=') + 2).split("/");
				long softBufferLimit = Long.parseLong(dataBuffer[0]); //398458880
				long kvbufferBytes = Long.parseLong(dataBuffer[1]); //498073600
				logWriter.println("softBufferLimit = " + softBufferLimit + ", kvbufferLen = " + kvbufferBytes);
				//mapper.getMapperBuffer().setDataBuffer(softBufferLimit, kvbufferBytes);
			}
			
			else if(syslog[i].contains("record buffer")) {
				String[] recordBuffer = syslog[i].substring(syslog[i].lastIndexOf('=') + 2).split("/");
				long softRecordLimit = Long.parseLong(recordBuffer[0]); //1310720
				long kvoffsetsLen = Long.parseLong(recordBuffer[1]); //1638400
				
				//mapper.getMapperBuffer().setRecordBuffer(softRecordLimit, kvoffsetsLen);
				logWriter.println("softRecordLimit = " + softRecordLimit + ", kvoffsetsLen = " + kvoffsetsLen);
				logWriter.println();
				i++;
				break;
			}		
		}
		/*
		 * 2012-10-10 14:59:41,167 INFO org.apache.hadoop.mapred.MapTask: Spilling map output: record full = true
		 * 2012-10-10 14:59:41,168 INFO org.apache.hadoop.mapred.MapTask: bufstart = 0; bufend = 13585067; bufvoid = 498073600
		 * 2012-10-10 14:59:41,168 INFO org.apache.hadoop.mapred.MapTask: kvstart = 0; kvend = 1310720; length = 1638400
		 * 2012-10-10 14:59:42,859 INFO org.apache.hadoop.mapred.MapTask: Finished spill 0 <RecordsBeforeCombine = 1310720, 
		 * BytesBeforeSpill = 13585067, RecordAfterCombine = 161552, RawLength = 2492902, CompressedLength = 2492966>
		 * or
		 * 2012-10-10 17:29:31,088 INFO org.apache.hadoop.mapred.MapTask: Finished spill 2 without combine <Records = 62916, 
		 * BytesBeforeSpill = 6291600, RawLength = 6417448, CompressedLength = 916344>
		 */
		long startSpillTimeMS = 0;
		String reason = "";
		int spillid = -1;
		
		for (; i < syslog.length; i++) {

			if(syslog[i].contains("Spilling map output")) {
				reason = "record";
				if(syslog[i].contains("buffer"))
					reason = "buffer";				
				startSpillTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:41
				logWriter.println("startSpillTimeMS = " + startSpillTimeMS + ", reason = " + reason);
				++spillid;
			
			} //end if(syslog[i].contains("Spilling map output")) {
			
			else if (syslog[i].contains("Starting flush of map output")) {
				reason = "flush";
				startSpillTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:41
				logWriter.println("startSpillTimeMS = " + startSpillTimeMS + ", reason = " + reason);
				++spillid;
			}
			
			//2012-12-05 16:17:35,669 INFO org.apache.hadoop.mapred.MapTask: [PartInfos][Partition 0]<RecordsBeforeCombine = 94671, 
			//RawLengthBeforeMerge = 9656444, RecordsAfterCombine = 94671, RawLength = 9656444, CompressedLength = 1404774>
			else if(syslog[i].contains("PartInfos")) {
				long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int partitionIdEnd = Integer.parseInt(syslog[i].substring(syslog[i].indexOf("Partition") + 10, syslog[i].lastIndexOf(']')));
				
				String valueString = syslog[i].substring(syslog[i].lastIndexOf('<') + 1, syslog[i].lastIndexOf('>'));
				long[] values = extractLongNumber(valueString, 5);
				
				long RecordsBeforeCombine = values[0];
				long RawLengthBeforeMerge = values[1];
				long RecordsAfterCombine = values[2];
				long RawLength = values[3];
				long CompressedLength = values[4];
				
				logWriter.println(syslog[i].substring(syslog[i].indexOf("[")));
			}
			
			else if(syslog[i].contains("Finished spill")) {
				int spillLoc = syslog[i].indexOf("spill") + 6;
				int spillId = Integer.parseInt(syslog[i].substring(spillLoc, syslog[i].indexOf(" ", spillLoc))); // 0
				assert(spillid == spillId);
				long stopSpillTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 14:59:42
				String valuesStr = syslog[i].substring(syslog[i].indexOf('<') + 1, syslog[i].lastIndexOf('>'));
				logWriter.print("[" + stopSpillTimeMS + "][" + spillId + "]");
				
				if(syslog[i].contains("without combine")) {
					long[] values = extractLongNumber(valuesStr, 4);
					long Records = values[0];// 1310720 
					long BytesBeforeSpill = values[1]; //13585067
					long RawLength = values[2]; //2492902
					long CompressedLength = values[3]; //2492966
					logWriter.println("[without combine] " + "Records = " + Records + ", BytesBeforeSpill = " + BytesBeforeSpill
							+ ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
					logWriter.println();
					//mapper.getSpill().addSpillItem(false, startSpillTimeMS, stopSpillTimeMS, reason, Records, BytesBeforeSpill, Records, 
					//		RawLength, CompressedLength);
				}
				else {
					long[] values = extractLongNumber(valuesStr, 5);
					long RecordsBeforeCombine = values[0]; // 1310720 
					long BytesBeforeSpill = values[1]; //13585067
					long RecordAfterCombine = values[2]; //161552
					long RawLength = values[3]; //2492902
					long CompressedLength = values[4]; //2492966
					
					logWriter.println("[with combine] " + "RecordsBeforeCombine = " + RecordsBeforeCombine + ", BytesBeforeSpill = "
							+ BytesBeforeSpill + ", RecordAfterCombine = " + RecordAfterCombine + ", RawLength = " + RawLength
							+ ", CompressedLength = " + CompressedLength);
					logWriter.println();
					//mapper.getSpill().addSpillItem(true, startSpillTimeMS, stopSpillTimeMS, reason, RecordsBeforeCombine, BytesBeforeSpill,
					//		RecordAfterCombine, RawLength, CompressedLength);
					
				}
			}
			else if(syslog[i].contains("[BeforeMerge]")) {
				long startMergePhaseTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				logWriter.println("----------------------------------------------------------------");
				logWriter.println("startMergePhaseTimeMS = " + startMergePhaseTimeMS);
				//mapper.setMergePhaseStartTimeMS(startMergePhaseTimeMS);
				break;
			}	
		} //end for (; i < syslog.length; i++)
		
		/*
		 * 2012-10-10 15:00:45,065 INFO org.apache.hadoop.mapred.MapTask: [BeforeMerge][Partition 12]<SegmentsNum = 32, RawLength = 4852329, CompressedLength = 4852457>
		 * 2012-10-10 15:00:45,065 INFO org.apache.hadoop.mapred.Merger: Merging 32 sorted segments
		 * 2012-10-10 15:00:45,090 INFO org.apache.hadoop.mapred.Merger: Merging 5 intermediate segments out of a total of 32 <WriteRecords = 49602, RawLength = 760991, CompressedLength = 760995>
		 * 2012-10-10 15:00:45,141 INFO org.apache.hadoop.mapred.Merger: Merging 10 intermediate segments out of a total of 28 <WriteRecords = 96566, RawLength = 1476601, CompressedLength = 1476605>
		 * 2012-10-10 15:00:45,193 INFO org.apache.hadoop.mapred.Merger: Merging 10 intermediate segments out of a total of 19 <WriteRecords = 98732, RawLength = 1519801, CompressedLength = 1519805>
		 * 2012-10-10 15:00:45,195 INFO org.apache.hadoop.mapred.Merger: Down to the last merge-pass, with 10 segments left of total size: 4852285 bytes
		 * 2012-10-10 15:00:45,325 INFO org.apache.hadoop.mapred.MapTask: [AfterMergeAndCombine][Partition 12]<RecordsBeforeCombine = 316038, 
		 * RecordsAfterCombine = 120636, RawLength = 1986498, CompressedLength = 1986502>
		 */
		for(; i < syslog.length; i++) {
			if(syslog[i].contains("[BeforeMerge]")) {
				long startMergeTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(',')));
				int partitionIdStart = Integer.parseInt(syslog[i].substring(syslog[i].indexOf("Partition") + 10, syslog[i].lastIndexOf(']')));
				String valueStr = syslog[i].substring(syslog[i].lastIndexOf('<') + 1, syslog[i].lastIndexOf('>'));
				
				long[] values = extractLongNumber(valueStr, 3);
				int SegmentsNum = (int)values[0]; //32
				long RawLength = values[1]; //4852329
				long CompressedLength = values[2]; //4852457
				logWriter.println("[BeforeMerge][" + partitionIdStart + "][" + startMergeTimeMS + "] SegmentsNum = " +
						SegmentsNum + ", RawLength = " + RawLength + ", CompressedLength = " + CompressedLength);
				//mapper.getMerge().addBeforeMergeItem(startMergeTimeMS, partitionIdStart, SegmentsNum, RawLength, CompressedLength);
				
				
				for(int j = i + 1; j < syslog.length; j++) {
					if(syslog[j].contains("intermediate segments")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int mergeSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Merging") + 8, syslog[j].indexOf("intermediate") - 1));
						int totalSegmentsNum = Integer.parseInt(syslog[j].substring(syslog[j].lastIndexOf("of") + 3, syslog[j].lastIndexOf('<') -1));
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						values = extractLongNumber(valueString, 3);
						
						long writeRecords = values[0];
						long rawLengthInter = values[1];
						long compressedLengthInter = values[2];
						
						//mapper.getMerge().addIntermediateMergeItem(mergeTimeMS, mergeSegmentsNum, totalSegmentsNum, writeRecords, 
						//		rawLengthInter, compressedLengthInter);
						logWriter.println("[" + mergeTimeMS + "] mergeSegmentsNum = " + mergeSegmentsNum + ", totalSegmentsNum = "
								+ totalSegmentsNum + ", WriteRecords = " + writeRecords + ", RawLength = " + rawLengthInter
								+ ", CompressedLength = " + compressedLengthInter); 
					}
					else if(syslog[j].contains("Down to the last merge-pass")) {
						long mergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						String valueString = syslog[j].substring(syslog[j].indexOf("Down"));
						values = extractLongNumber(valueString, 2);
						
						int lastMergePassSegmentsNum = (int)values[0];
						long lastMergePassTotalSize = values[1];
						
						//mapper.getMerge().addLastMergePassItem(mergeTimeMS, lastMergePassSegmentsNum, lastMergePassTotalSize);
						logWriter.println("[" + mergeTimeMS + "] lastMergePassSegmentsNum = " + lastMergePassSegmentsNum
								+ ", lastMergePassTotalSize = " + lastMergePassTotalSize); 
					}
					else if(syslog[j].contains("AfterMergeAndCombine")) {
						long stopMergeTimeMS = DateParser.parseLogTimeMS(syslog[j].substring(0, syslog[j].indexOf(',')));
						int partitionIdEnd = Integer.parseInt(syslog[j].substring(syslog[j].indexOf("Partition") + 10, syslog[j].lastIndexOf(']')));
						assert(partitionIdStart == partitionIdEnd);
						String valueString = syslog[j].substring(syslog[j].lastIndexOf('<') + 1, syslog[j].lastIndexOf('>'));
						values = extractLongNumber(valueString, 4);
						
						long RecordsBeforeMerge = values[0]; //316038 
						long RecordsAfterMerge = values[1]; //120636
						long RawLengthEnd = values[2]; //1986498
						long CompressedLengthEnd = values[3]; //1986502
						
						//mapper.getMerge().addAfterMergeItem(stopMergeTimeMS, partitionIdEnd, RecordsBeforeMerge, RecordsAfterMerge, RawLengthEnd, CompressedLengthEnd);
						logWriter.println("[AfterMergeAndCombine][" + partitionIdEnd + "][" + stopMergeTimeMS + "] RecordsBeforeMerge = "
								+ RecordsBeforeMerge + ", RecordsAfterMerge = " + RecordsAfterMerge + ", RawLength = " + RawLengthEnd
								+ ", CompressedLength = " + CompressedLengthEnd);
						logWriter.println();
						i = j;
						break;
					}
				}
			}
			/*
			 * 2012-10-10 17:29:41,925 INFO org.apache.hadoop.mapred.TaskRunner: Task:attempt_201210101630_0003_m_000000_0 is done. And is in the process of commiting
			 * 2012-10-10 17:29:41,929 INFO org.apache.hadoop.mapred.TaskRunner: Task 'attempt_201210101630_0003_m_000000_0' done.
			 */
			else if(syslog[i].contains("done. And is in the process of commiting")) {
				long mapperStopTimeMS = DateParser.parseLogTimeMS(syslog[i].substring(0, syslog[i].indexOf(','))); //2012-10-10 17:29:41
				logWriter.println("mapperStopTimeMS = " + mapperStopTimeMS);
				//mapper.setMapperStopTimeMS(mapperStopTimeMS);
				break;
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
