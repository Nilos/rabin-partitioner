package de.hpi.cloudraid.rabin_partitioner;

public class OutputLogger {
	public static void log(String text) {
		System.out.println(text);
	}

	public static void error(String text) {
		// TODO Auto-generated method stub
		OutputLogger.log(text);
		System.err.println(text);
	}
}
