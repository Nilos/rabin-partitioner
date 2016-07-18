package de.hpi.cloudraid.rabin_partitioner;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;

import de.hpi.cloudraid.rabin_partitioner.FilePart;

public class PartitionService {

	public static final int RABINWINDOWSIZE = 48;

	private int minPartSize = 1 * 1024;
	private static final int MAXPARTSIZE = 64 * 1024;
	// 13 lowest bits. Means likelihood of breakpoint is 2^-13 which leads to
	// average size of 8KB ( + MinSize + windowSize)
	private long fingerPrintBitMask = 0x1FFFL;

	private static BlockingQueue<RabinFingerprintLongWindowed> rabinWindowResources = new ArrayBlockingQueue<RabinFingerprintLongWindowed>(4);
	
	private final File file;
	private final InputStream fileStream;

	private RabinFingerprintLongWindowed rabinWindow;

	public static void generateRabinWindows() throws InterruptedException {
		RabinWindowGenerator myRunnable = new RabinWindowGenerator(rabinWindowResources);
        Thread t = new Thread(myRunnable);
        t.start();
        t.join();
	}
	
	private RabinFingerprintLongWindowed getRabinWindow() throws InterruptedException {
		return rabinWindowResources.take();
	}
	
	private void releaseRabinWindow() throws InterruptedException {
		if (this.rabinWindow == null) {
			return;
		}
		
		RabinFingerprintLongWindowed rabinWindow = this.rabinWindow;
		this.rabinWindow = null;
		
		if (rabinWindowResources.remainingCapacity() == 0) {
			throw new RuntimeException("Could not release rabin window!");
		}
		
		rabinWindowResources.put(rabinWindow);
	}
	
	public PartitionService(File file) throws FileNotFoundException {
		this.file = file;
		
		this.fileStream = new BufferedInputStream(new FileInputStream(this.file), MAXPARTSIZE * 2);
	}
	
	public PartitionService(File file, int minPartSize, long fingerPrintBitMask) throws FileNotFoundException {
		this.file = file;
		
		this.minPartSize = minPartSize;
		this.fingerPrintBitMask = fingerPrintBitMask;

		this.fileStream = new BufferedInputStream(new FileInputStream(this.file), MAXPARTSIZE * 2);
	}

	public Optional<FilePart> getNextPart() throws IOException, InterruptedException {
		return this.findRabinWindow(this.fileStream);
	}

	private byte[] readBytes(InputStream stream, int numberOfBytesMax) throws IOException {
		byte[] readBytes = new byte[numberOfBytesMax];

		int bytesRead = stream.read(readBytes);

		if (bytesRead == -1) {
			return new byte[0];
		}

		return Arrays.copyOfRange(readBytes, 0, bytesRead);
	}

	private Optional<FilePart> findRabinWindow(InputStream stream) throws IOException, InterruptedException {
		int partSize = 0;
		byte[] startBytes = this.readBytes(stream, minPartSize);

		if (startBytes.length == 0) {
			this.releaseRabinWindow();
			return Optional.empty();
		}

		if (startBytes.length < minPartSize) {
			return Optional.of(new FilePart(startBytes));
		}

		partSize += startBytes.length;
		
		if (rabinWindow == null) {
			rabinWindow = getRabinWindow();
		}

		rabinWindow.pushBytes(startBytes, startBytes.length - RABINWINDOWSIZE - 1, RABINWINDOWSIZE);

		byte[] readByte = new byte[1];

		ByteArrayOutputStream partStream = new ByteArrayOutputStream();

		partStream.write(startBytes);

		int numberOfBytesRead;
		while ((numberOfBytesRead = stream.read(readByte)) != -1) {
			if (numberOfBytesRead > 0) {
				partSize += numberOfBytesRead;

				rabinWindow.pushByte(readByte[0]);
				partStream.write(readByte);

				long calculatedFingerPrint = rabinWindow.getFingerprintLong() & fingerPrintBitMask;

				if (calculatedFingerPrint == 0L) {
					//System.err.println(String.format("Fingerprint: %X , Part Size: %d", this.rabinWindow.getFingerprintLong(), partSize));

					break;
				}

				if (partSize >= MAXPARTSIZE) {
					break;
				}
			}
		}

		rabinWindow.reset();
		Optional<FilePart> result = Optional.of(new FilePart(partStream.toByteArray()));

		partStream.close();

		return result;
	}
}
