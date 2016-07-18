package de.hpi.cloudraid.rabin_partitioner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class DirectoryPartitioner {
	private HashMap<String, FilePart> partsMap = new HashMap<String, FilePart>();
	private AtomicLong totalBytes = new AtomicLong(0);
	private AtomicLong deduplicatedBytes = new AtomicLong(0);
	private AtomicLong deduplicatedBytesWholeFiles = new AtomicLong(0);
	private long start;
	private AtomicLong previousTotalBytes = new AtomicLong(0);
	private AtomicLong totalPartsCount = new AtomicLong(0);
	private File folder;
	
	public DirectoryPartitioner(File folder) {
		this.folder = folder;
	}
	
    public void run() throws IOException, InterruptedException
    {
    	//read all files in the current dir and rabin fingerprint partition them into a hashmap (we do not need the content, only the hash and length)
    	//find out how many of those parts can be deduplicated - print out that number
    	//print out a speed (MB/s)
        
        start = System.currentTimeMillis();
        PartitionService.generateRabinWindows();
        findDeduplicableParts(this.folder);

        printResults();
        System.out.println("Done scanning directory!");
    }
    
    private void printResults() {
    	long took = System.currentTimeMillis() - start;
    	long deduplicatedRabin = deduplicatedBytes.get() - deduplicatedBytesWholeFiles.get();
    	
    	String Bs = humanReadableByteCount((long) Math.floor(totalBytes.get() / (took / 1000.0)));
    	
    	System.out.println("Results for directory: " + this.folder.getName());
    	System.out.println(String.format("Took %d ms (%s/s)", took, Bs));
        System.out.println(String.format("Bytes deduplicated: %s of %s (%d percent)", humanReadableByteCount(deduplicatedBytes.get()), humanReadableByteCount(totalBytes.get()), percent(deduplicatedBytes.get(), totalBytes.get())));
        System.out.println(String.format("Bytes deduplicated via rabin: %s of %s (%d percent)", humanReadableByteCount(deduplicatedRabin), humanReadableByteCount(totalBytes.get()), percent(deduplicatedRabin, totalBytes.get())));
        System.out.println(String.format("Average part size: %s - Number of parts: %d", 
        	humanReadableByteCount((int) Math.floor(totalBytes.get() / totalPartsCount.get())),
        	totalPartsCount.get()
        ));
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
    
    private synchronized boolean checkDeduplication(FilePart thePart) {
    	if (partsMap.containsKey(thePart.getHash())) {
    		return true;
    	}
    	
    	partsMap.put(thePart.getHash(), thePart);
    	return false;
    }
    
	private void findDeduplicableParts(File folder) throws IOException, InterruptedException {
		File[] files = folder.listFiles();
		
		Arrays.asList(files).parallelStream().forEach((File file) -> {
			if (file.getName().startsWith(".")) {
				return;
			}

			try {
				if (file.isDirectory()) {
					findDeduplicableParts(file);
				} else {
					PartitionService partitionService = new PartitionService(file);
					
					long deduplicatedFileBytes = 0;
					long fileBytes = 0;
					long partsCount = 0;
					
					while (true) {
						Optional<FilePart> nextPart = partitionService.getNextPart();
								
						if (!nextPart.isPresent()) {
							break;
						}
						
						FilePart thePart = nextPart.get();

						fileBytes += thePart.getLength();
						
						if (checkDeduplication(thePart)) {
							deduplicatedFileBytes += thePart.getLength();
						}
						
						partsCount++;
					}
					
					totalPartsCount.addAndGet(partsCount);
					totalBytes.addAndGet(fileBytes);
					deduplicatedBytes.addAndGet(deduplicatedFileBytes);
					if (fileBytes == deduplicatedFileBytes) {
						deduplicatedBytesWholeFiles.addAndGet(fileBytes);
					}
				}
				
				if (totalBytes.get() > previousTotalBytes.get() + 100 * 1024 * 1024) {
					previousTotalBytes.set(totalBytes.get());
					//printResults();
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		});
	}
}
