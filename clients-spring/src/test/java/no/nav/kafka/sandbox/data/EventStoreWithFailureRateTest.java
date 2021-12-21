package no.nav.kafka.sandbox.data;

import no.nav.kafka.sandbox.data.EventStoreWithFailureRate.FixedFailSuccessCountPattern;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EventStoreWithFailureRateTest {

    @Test
    public void testFixedFailSuccessCountStrategy() {
        FixedFailSuccessCountPattern<String> fixedFailSuccessCountPattern =
                FixedFailSuccessCountPattern.fromSpec("2 / 3");

        assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        assertFalse(fixedFailSuccessCountPattern.failureFor("any"));
        assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        assertFalse(fixedFailSuccessCountPattern.failureFor("any"));

        fixedFailSuccessCountPattern = FixedFailSuccessCountPattern.fromSpec("1/1");
        assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
    }

    @Test
    public void testFixedFailSuccessCountStrategy_5_10() {
        FixedFailSuccessCountPattern<String> fixedFailSuccessCountPattern =
                FixedFailSuccessCountPattern.fromSpec("5/10");
        for (int i=0; i<5; i++) {
            assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        }
        for (int i=0; i<5; i++) {
            assertFalse(fixedFailSuccessCountPattern.failureFor("any"));
        }
        for (int i=0; i<5; i++) {
            assertTrue(fixedFailSuccessCountPattern.failureFor("any"));
        }
    }

}
