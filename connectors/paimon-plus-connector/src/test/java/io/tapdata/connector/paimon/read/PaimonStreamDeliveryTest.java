package io.tapdata.connector.paimon.read;

import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class PaimonStreamDeliveryTest {
    final List<List<TapEvent>> batches = new ArrayList<>();
    final List<PaimonReadOffset> offsets = new ArrayList<>();
    final AtomicLong clock = new AtomicLong();
    final PaimonStreamReadSession session = new PaimonStreamReadSession(PaimonReadOffsetTest.offset(), 2, 10,
            () -> true, (events, offset) -> { batches.add(events); offsets.add(PaimonReadOffset.parse(offset)); }, clock::get);
    PaimonStreamReadCoordinator.Item event() { return PaimonStreamReadCoordinator.Item.event("orders", new TapInsertRecordEvent().init()); }
    @Test void fullBatchesKeepOldOffsetUntilCompletePlanAndPublishedListsRemainStable() {
        session.deliver(event()); session.deliver(event());
        assertEquals(8, offsets.get(0).position("orders").nextSnapshot);
        session.deliver(event()); session.deliver(PaimonStreamReadCoordinator.Item.finished("orders", 8, 9));
        assertEquals(9, offsets.get(1).position("orders").nextSnapshot);
        assertEquals(2, batches.get(0).size()); assertEquals(1, batches.get(1).size());
        assertEquals(2, offsets.get(1).position("unrequested").nextSnapshot);
    }
    @Test void exactBatchBoundaryAndZeroEventPlanUseHeartbeatNotEmptyList() {
        session.deliver(event()); session.deliver(event());
        session.deliver(PaimonStreamReadCoordinator.Item.finished("orders", 8, 9));
        assertTrue(batches.get(1).get(0) instanceof HeartbeatEvent);
        assertNotNull(batches.get(1).get(0).getTime());
        session.deliver(PaimonStreamReadCoordinator.Item.finished("orders", 9, 11));
        assertEquals(11, offsets.get(2).position("orders").nextSnapshot);
        assertTrue(batches.stream().noneMatch(List::isEmpty));
    }
    @Test void lowTrafficFlushIsMeasuredFromFirstUnsubmittedEvent() {
        session.deliver(event()); clock.set(9_000_000L); session.flushIfDue(); assertTrue(batches.isEmpty());
        clock.set(10_000_000L); session.flushIfDue(); assertEquals(1, batches.size());
        assertEquals(8, offsets.get(0).position("orders").nextSnapshot);
    }
    @Test void callbackFailureDoesNotAdvanceAndPreventsFurtherAdmission() {
        RuntimeException failure = new RuntimeException("downstream");
        PaimonStreamReadSession failing = new PaimonStreamReadSession(PaimonReadOffsetTest.offset(), 2, 10,
                () -> true, (events, offset) -> { throw failure; });
        assertSame(failure, assertThrows(RuntimeException.class, () -> failing.deliver(PaimonStreamReadCoordinator.Item.finished("orders", 8, 9))));
        assertEquals(8, failing.submittedOffset().position("orders").nextSnapshot);
        assertThrows(CancellationException.class, failing::admit);
        assertEquals(0, failing.inFlight());
    }
    @Test void cancellationDuringCallbackIsNotAcknowledgement() {
        AtomicBoolean running = new AtomicBoolean(true);
        PaimonStreamReadSession stopped = new PaimonStreamReadSession(PaimonReadOffsetTest.offset(), 2, 10,
                running::get, (events, offset) -> running.set(false));
        assertThrows(CancellationException.class, () -> stopped.deliver(PaimonStreamReadCoordinator.Item.finished("orders", 8, 9)));
        assertEquals(8, stopped.submittedOffset().position("orders").nextSnapshot);
        assertEquals(0, stopped.inFlight());
    }
    @Test void admissionBeforeCancelRemainsTrackedButNoNewTicketIsIssued() {
        PaimonStreamReadSession.Admission ticket = session.admit(); session.cancel();
        assertEquals(1, session.inFlight()); assertThrows(CancellationException.class, session::admit);
        ticket.close(); ticket.close(); assertEquals(0, session.inFlight());
    }
}
