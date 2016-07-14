package de.hpi.cloudraid.rabin_partitioner;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class FilePart {

	private int length;
	private String hash;

	public FilePart(byte[] content) {
		try {
			this.hash = FilePart.calculateHash(content);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(2);
		}
		this.length = content.length;
	}

	public static String calculateHash(byte[] content) throws NoSuchAlgorithmException {
		MessageDigest md = MessageDigest.getInstance("SHA-256");
		md.update(content);
		
		return String.format("%064x", new java.math.BigInteger(1, md.digest()));
	}
	
	public String getHash() {
		return this.hash;
	}

	public int getLength() {
		return this.length;
	}
}
