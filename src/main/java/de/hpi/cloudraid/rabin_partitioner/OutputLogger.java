package de.hpi.cloudraid.rabin_partitioner;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class OutputLogger {
	private static File file;
	private static FileWriter fw;
	private static BufferedWriter bw;
	
	public static void open() throws IOException {
		file = new File(System.getProperty("user.home")).toPath().resolve("Desktop").resolve("Rabin-Partitioner-Nils-MA.log").toFile();
		
		file.createNewFile();
		
		fw = new FileWriter(file.getAbsoluteFile());
		bw = new BufferedWriter(fw);
	}
	
	public static void close() throws IOException {
		bw.close();
	}
	
	public static void log(String text) {
		System.out.println(text);

		try {
			bw.write(text + "\n");
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(5);
		}
	}

	public static void error(String text) {
		OutputLogger.log(text);
		System.err.println(text);
	}
}
