package no.nav.kafka.sandbox.messages;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Supplier and consumer that does message sequence validation.
 *
 * <p>Can be used to test loss or reordering of Kafka messages</p>
 *
 * <p>The supplier maintains state on disk wrt. next sequence number. Delete the persistence file to reset state.</p>
 */
public class SequenceValidation {

    public static Supplier<Long> sequenceSupplier(File persistence, long delay, TimeUnit timeUnit) {
        return new SequenceSupplier(persistence, delay, timeUnit);
    }

    public static Consumer<Long> sequenceValidatorConsolePrinter() {
        return new SequenceValidator();
    }

    private static class SequenceSupplier implements Supplier<Long> {
        final AtomicLong sequence;
        final long delayMillis;
        final File persistence;

        private SequenceSupplier(File persistence, long delay, TimeUnit timeUnit) {
            this.delayMillis = timeUnit.toMillis(delay);
            this.persistence = persistence;
            this.sequence = new AtomicLong(loadValue(0));
        }

        private void saveValue(long value) {
            try (DataOutputStream out = new DataOutputStream(new FileOutputStream(persistence))) {
                out.writeLong(value);
            } catch (Exception e) {}
        }

        private long loadValue(long defaultValue) {
            try (DataInputStream in = new DataInputStream(new FileInputStream(persistence))) {
                return in.readLong();
            } catch (Exception e) {
                return defaultValue;
            }
        }

        @Override
        public Long get() {
            try {
                Thread.sleep(delayMillis);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            final long toBeDelivered = sequence.longValue();
            toConsole("[SEQ] Supplying sequence(%d)", toBeDelivered);
            saveValue(sequence.incrementAndGet());
            return toBeDelivered;
        }
    }

    private static class SequenceValidator implements Consumer<Long> {

        long expect = -1;
        long errorCount = 0;

        @Override
        public void accept(Long received) {
            if (expect == -1) {
                expect = received + 1;
                toConsole("[SEQ] Synchronized sequence: received(%d), next expect(%d), errors(%d)", received, expect, errorCount);
            } else if (received < expect) {
                ++errorCount;
                toConsole("[SEQ] ERROR: Received lower sequence than expected: received(%d) < expect(%d), resync next, errors(%d)",
                        received, expect, errorCount);
                expect = -1;
            } else if  (received > expect){
                ++errorCount;
                toConsole("[SEQ] ERROR: Received higher sequence than expected: received(%d) > expect(%d), resync next, errors(%d)",
                        received, expect, errorCount);
                expect = -1;
            } else {
                toConsole("[SEQ] In sync: received(%d) = expect(%d), errors(%d)", received, expect, errorCount);
                ++expect;
            }
        }
    }

    private static void toConsole(String format, Object...args) {
        System.out.println(String.format(format, args));
    }
}
