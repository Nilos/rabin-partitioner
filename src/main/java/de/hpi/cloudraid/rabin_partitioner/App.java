package de.hpi.cloudraid.rabin_partitioner;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

/**
 * Hello world!
 *
 */
public class App 
{
	private static HashMap<String, FilePart> partsMap = new HashMap<String, FilePart>();
	private static long totalBytes = 0;
	private static long deduplicatedBytes = 0;
	private static long deduplicatedBytesWholeFiles = 0;
	private static long start;
	private static long previousTotalBytes;
	
    public static void main( String[] args ) throws IOException, InterruptedException
    {
    	//read all files in the current dir and rabin fingerprint partition them into a hashmap (we do not need the content, only the hash and length)
    	//find out how many of those parts can be deduplicated - print out that number
    	//print out a speed (MB/s)

    	if (args.length == 0) {
    		System.err.println("Need a path to work on!");
    		System.exit(1);
    	}

        File folder = new File(args[0]);
        if (!folder.isDirectory()) {
    		System.err.println("Need a directory to work on!");
    		System.exit(1);
        }
        
        start = System.currentTimeMillis();
        findDeduplicableParts(folder);

        printResults();
        
        System.out.println("Done scanning directory!");
    }
    
    private static void printResults() {
    	long took = System.currentTimeMillis() - start;
    	long deduplicatedRabin = deduplicatedBytes - deduplicatedBytesWholeFiles;
    	
    	String Bs = humanReadableByteCount((long) Math.floor(totalBytes / (took / 1000.0)));
    	
    	System.out.println(String.format("Took %d ms (%s/s)", took, Bs));
        System.out.println(String.format("Bytes deduplicated: %s of %s (%d percent)", humanReadableByteCount(deduplicatedBytes), humanReadableByteCount(totalBytes), percent(deduplicatedBytes, totalBytes)));
        System.out.println(String.format("Bytes deduplicated via rabin: %s of %s (%d percent)", humanReadableByteCount(deduplicatedRabin), humanReadableByteCount(totalBytes), percent(deduplicatedRabin, totalBytes)));
    }

    public static String humanReadableByteCount(long bytes) {
        int unit = 1024;
        if (bytes < unit) return bytes + "B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        char pre = "KMGTPE".charAt(exp-1);
        return String.format("%.1f%sB", bytes / Math.pow(unit, exp), pre);
    }
    
    private static int percent(long x, long y) {
    	return (int) Math.floor(100.0 * x / y);
    }
    
	private static void findDeduplicableParts(File folder) throws IOException, InterruptedException {
		File[] files = folder.listFiles();
		
		for (File file : files) {
			if (file.getName().startsWith(".")) {
				continue;
			}

			if (file.isDirectory()) {
				findDeduplicableParts(file);
			} else {
				PartitionService partitionService = new PartitionService(file);
				
				int deduplicatedFileBytes = 0;
				int fileBytes = 0;
				
				while (true) {
					Optional<FilePart> nextPart = partitionService.getNextPart();
							
					if (!nextPart.isPresent()) {
						break;
					}
					
					FilePart thePart = nextPart.get();
					
					totalBytes += thePart.getLength();
					fileBytes += thePart.getLength();
					
					if (partsMap.containsKey(thePart.getHash())) {
						deduplicatedBytes += thePart.getLength();
						deduplicatedFileBytes += thePart.getLength();
					} else {
						partsMap.put(thePart.getHash(), thePart);
					}
				}
				
				if (fileBytes == deduplicatedFileBytes) {
					deduplicatedBytesWholeFiles += fileBytes;
				}
			}
			
			if (totalBytes > previousTotalBytes + 1024 * 1024) {
				previousTotalBytes = totalBytes;
				printResults();
			}
		}
	}
}
