package de.hpi.cloudraid.rabin_partitioner;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Optional;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.polynomial.Polynomial;

import de.hpi.cloudraid.rabin_partitioner.FilePart;

public class PartitionService {

	private static final int RABINWINDOWSIZE = 48;

	private static final int MINPARTSIZE = 1 * 1024;
	private static final int MAXPARTSIZE = 64 * 1024;
	// 13 lowest bits. Means likelihood of breakpoint is 2^-13 which leads to
	// average size of 8KB ( + MinSize + windowSize)
	private static final long FINGERPRINTBITMASK = 0x1FFFL;

	private final File file;
	private final InputStream fileStream;
	private RabinFingerprintLongWindowed rabinWindow;

	public PartitionService(File file) throws FileNotFoundException {
		this.file = file;

		this.fileStream = new BufferedInputStream(new FileInputStream(this.file), MAXPARTSIZE * 2);
	}

	public Optional<FilePart> getNextPart() throws IOException {
		return this.partitionRabin();
	}

	private byte[] readBytes(InputStream stream, int numberOfBytesMax) throws IOException {
		byte[] readBytes = new byte[numberOfBytesMax];

		int bytesRead = stream.read(readBytes);

		if (bytesRead == -1) {
			return new byte[0];
		}

		return Arrays.copyOfRange(readBytes, 0, bytesRead);
	}

	private Optional<FilePart> partitionRabin() throws IOException {
		return this.findRabinWindow(this.fileStream);
	}

	private Optional<FilePart> findRabinWindow(InputStream stream) throws IOException {
		int partSize = 0;
		byte[] startBytes = this.readBytes(stream, MINPARTSIZE);

		if (startBytes.length == 0) {
			return Optional.empty();
		}

		if (startBytes.length < MINPARTSIZE) {
			return Optional.of(new FilePart(startBytes));
		}

		partSize += startBytes.length;

		if (this.rabinWindow == null) {
			// Create new random irreducible polynomial
			// These can also be created from Longs or hex Strings
			Polynomial polynomial = Polynomial.createFromLong(0x23E5CB30495711L);

			// Create a windowed fingerprint object with a window size of 48
			// bytes.
			this.rabinWindow = new RabinFingerprintLongWindowed(polynomial, RABINWINDOWSIZE);
		}

		rabinWindow.pushBytes(startBytes, startBytes.length - RABINWINDOWSIZE - 1, RABINWINDOWSIZE);

		byte[] readByte = new byte[1];

		ByteArrayOutputStream partStream = new ByteArrayOutputStream();

		partStream.write(startBytes);

		int numberOfBytesRead;
		while ((numberOfBytesRead = stream.read(readByte)) != -1) {
			if (numberOfBytesRead > 0) {
				partSize += numberOfBytesRead;

				this.rabinWindow.pushByte(readByte[0]);
				partStream.write(readByte);

				long calculatedFingerPrint = this.rabinWindow.getFingerprintLong() & FINGERPRINTBITMASK;

				if (calculatedFingerPrint == 0L) {
					//System.err.println(String.format("Fingerprint: %X", this.rabinWindow.getFingerprintLong()));
					//System.err.println(String.format("GOT A PART BORDER WITH RABIN FINGERPRINT %d", partSize));

					rabinWindow.reset();

					break;
				}

				if (partSize >= MAXPARTSIZE) {
					break;
				}
			}
		}

		Optional<FilePart> result = Optional.of(new FilePart(partStream.toByteArray()));

		partStream.close();

		return result;
	}

	public static int getMinPartSize() {
		return MINPARTSIZE;
	}
}
