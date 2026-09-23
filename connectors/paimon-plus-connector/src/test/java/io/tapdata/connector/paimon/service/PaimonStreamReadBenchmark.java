package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.read.PaimonReadOffset;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Reads real local Paimon files through Service/session, but the downstream is only a callback. */
public final class PaimonStreamReadBenchmark {
    public static void main(String[] args) throws Exception {
        int rows = 20_000;
        try (PaimonStreamReadIntegrationTest.Fixture fixture = new PaimonStreamReadIntegrationTest.Fixture(false, Collections.emptyMap())) {
            String[] values = new String[rows];
            for (int i = 0; i < rows; i++) values[i] = "value-" + i;
            fixture.batch(false, values);
            for (int batch : new int[]{100, 1000, 10000}) {
                for (int delayMillis : new int[]{0, 2}) {
                    for (int run = -1; run < 5; run++) {
                        AtomicBoolean active = new AtomicBoolean(true);
                        int[] count = {0};
                        List<Long> callbackNanos = new ArrayList<>();
                        long start = System.nanoTime();
                        fixture.service.streamRead(Collections.singletonList("t"), fixture.start, batch,
                                StreamReadConsumer.create((events, offset) -> {
                                    long entered = System.nanoTime();
                                    for (TapEvent event : events) {
                                        if (event instanceof TapInsertRecordEvent) {
                                            if (((TapInsertRecordEvent) event).getAfter().get("v") == null) throw new AssertionError("missing value");
                                            count[0]++;
                                        }
                                    }
                                    if (delayMillis > 0) {
                                        try { Thread.sleep(delayMillis); }
                                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
                                    }
                                    callbackNanos.add(System.nanoTime() - entered);
                                    if (PaimonReadOffset.parse(offset).position("t").nextSnapshot == 2) active.set(false);
                                }), fixture.context, active::get);
                        long elapsed = System.nanoTime() - start;
                        if (count[0] != rows) throw new AssertionError("incorrect delivered count " + count[0]);
                        Collections.sort(callbackNanos);
                        if (run >= 0) System.out.printf(Locale.ROOT,
                                "stream,rows=%d,batch=%d,delay_ms=%d,run=%d,elapsed_ns=%d,callbacks=%d,callback_p95_ns=%d%n",
                                rows, batch, delayMillis, run, elapsed, callbackNanos.size(),
                                callbackNanos.get((int)Math.ceil(callbackNanos.size() * 0.95) - 1));
                    }
                }
            }
        }
    }
}
