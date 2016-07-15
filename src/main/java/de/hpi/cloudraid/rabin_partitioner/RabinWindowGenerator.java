package de.hpi.cloudraid.rabin_partitioner;

import java.util.concurrent.BlockingQueue;

import org.rabinfingerprint.fingerprint.RabinFingerprintLongWindowed;
import org.rabinfingerprint.polynomial.Polynomial;

public class RabinWindowGenerator implements Runnable {

    private BlockingQueue<RabinFingerprintLongWindowed> queue;

    public RabinWindowGenerator(BlockingQueue<RabinFingerprintLongWindowed> queue) {
        this.queue = queue;
    }

    public void run() {
		// Create new random irreducible polynomial
		// These can also be created from Longs or hex Strings
		Polynomial polynomial = Polynomial.createFromLong(0x23E5CB30495711L);
    	
		int totalWindows = this.queue.remainingCapacity();
		for (int i = 0; i < totalWindows; i += 1) {
			try {
				this.queue.put(new RabinFingerprintLongWindowed(polynomial, PartitionService.RABINWINDOWSIZE));
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(3);
			}
		}
    }
}