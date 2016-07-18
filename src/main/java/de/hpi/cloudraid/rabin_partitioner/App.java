package de.hpi.cloudraid.rabin_partitioner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedList;


/**
 * Hello world!
 *
 */
public class App 
{
	private static String[] knownCloudFolders = {"ownCloud", "dropBox", "Google Drive"};
    public static void main( String[] args ) throws IOException, InterruptedException
    {
    	//read all files in the current dir and rabin fingerprint partition them into a hashmap (we do not need the content, only the hash and length)
    	//find out how many of those parts can be deduplicated - print out that number
    	//print out a speed (MB/s)

    	LinkedList<File> folders = new LinkedList<File>();
    	
    	for (String filePath : args) {
            File folder = new File(filePath);
            if (!folder.isDirectory()) {
        		System.err.println(String.format("%s is not a directory!", filePath));
        		System.exit(1);
            }
            
            folders.add(folder);
    	}

    	if (folders.size() == 0) {
    		Path home = new File(System.getProperty("user.home")).toPath();
    		
    		for (String cloudFolder : knownCloudFolders) {
    			File folder = home.resolve(cloudFolder).toFile();
    			if (folder.exists() && folder.isDirectory()) {
    				folders.add(folder);
    			}
    		}
    	}
    	
    	if (folders.size() == 0) {
    		System.err.println("Need a path to work on!");
    		System.exit(1);
    	}
    	
    	long start = System.currentTimeMillis();
    	PartitionService.generateRabinWindows();
    	System.out.println(String.format("Calculating Rabin Windows took: %d", System.currentTimeMillis() - start));
    	
        folders.stream().forEach((File folder) -> {
        	System.out.println("Scanning folder: " + folder.getName());
        	try {
				new DirectoryPartitioner(folder).run(2 * 1024, 0x1FFFL);
				new DirectoryPartitioner(folder).run(4 * 1024, 0xFFFFL);
				System.out.println("Done scanning directory: " + folder.getName());
			} catch (Exception e) {
				System.out.println("Failed to scan directory: " + folder.getAbsolutePath());
				e.printStackTrace();
			}
        }); 
        
        System.out.println("Done scanning directories!");
    }
}
