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
	
    public static void main( String[] args ) throws IOException
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
    }
    
    private static void printResults() {
    	long took = System.currentTimeMillis() - start;
    	long deduplicatedRabin = deduplicatedBytes - deduplicatedBytesWholeFiles;
    	
    	double MBs = (totalBytes / 1024.0 / 1024.0) / (took / 1000.0);
    	
    	System.out.println(String.format("Took %d ms (%f MB/s)", took, MBs));
        System.out.println(String.format("Bytes deduplicated: %d of %d (%d percent)", deduplicatedBytes, totalBytes, percent(deduplicatedBytes, totalBytes)));
        System.out.println(String.format("Bytes deduplicated via rabin: %d of %d (%d percent)", deduplicatedRabin, totalBytes, percent(deduplicatedRabin, totalBytes)));
    }

    private static int percent(long x, long y) {
    	return (int) Math.floor(100.0 * x / y);
    }
    
	private static void findDeduplicableParts(File folder) throws IOException {
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
