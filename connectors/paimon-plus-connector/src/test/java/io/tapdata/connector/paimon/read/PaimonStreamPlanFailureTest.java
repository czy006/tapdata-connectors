package io.tapdata.connector.paimon.read;

import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PaimonStreamPlanFailureTest {
    final PaimonStreamReadCoordinator.Source source = mock(PaimonStreamReadCoordinator.Source.class);
    final List<PaimonStreamReadCoordinator.Item> items = new ArrayList<>();
    @SuppressWarnings("unchecked")
    RecordReader<InternalRow> setup() throws Exception {
        when(source.name()).thenReturn("orders"); when(source.checkpoint()).thenReturn(5L, 6L);
        TableScan.Plan plan = mock(TableScan.Plan.class);
        when(source.plan()).thenReturn(plan);
        when(plan.splits()).thenReturn(Arrays.asList(mock(Split.class), mock(Split.class)));
        RecordReader<InternalRow> reader = mock(RecordReader.class);
        RecordReader.RecordIterator<InternalRow> batch = mock(RecordReader.RecordIterator.class);
        when(source.reader(any())).thenReturn(reader);
        when(reader.readBatch()).thenReturn(batch, null);
        when(batch.next()).thenReturn(GenericRow.of(1), null);
        when(source.convert(any())).thenReturn(new TapInsertRecordEvent().init());
        return reader;
    }
    @Test void markerAppearsOnlyAfterAllReadersClose() throws Exception {
        RecordReader<InternalRow> reader = setup();
        AtomicBoolean closed = new AtomicBoolean(); doAnswer(i -> { closed.set(true); return null; }).when(reader).close();
        PaimonStreamReadCoordinator.readPlan(source, () -> true, item -> {
            if (item.isFinished()) { assertTrue(closed.get()); }
            items.add(item);
        });
        assertEquals(2, items.size()); assertTrue(items.get(1).isFinished());
        assertEquals(5, items.get(1).before); assertEquals(6, items.get(1).after);
        verify(reader, times(2)).close();
    }
    @Test void readerCloseFailureCannotPublishCompletedPlan() throws Exception {
        RecordReader<InternalRow> reader = setup();
        doThrow(new IOException("close failed")).when(reader).close();
        assertThrows(IOException.class, () -> PaimonStreamReadCoordinator.readPlan(source, () -> true, items::add));
        assertTrue(items.stream().noneMatch(PaimonStreamReadCoordinator.Item::isFinished));
    }
    @Test void conversionFailurePreservedAndBatchReleased() throws Exception {
        RecordReader<InternalRow> reader = setup();
        IOException first = new IOException("convert"); when(source.convert(any())).thenThrow(first);
        doThrow(new IOException("close")).when(reader).close();
        assertSame(first, assertThrows(IOException.class, () -> PaimonStreamReadCoordinator.readPlan(source, () -> true, items::add)));
        assertEquals(1, first.getSuppressed().length);
        assertTrue(items.isEmpty());
    }
    @Test void cancellationAfterPartialDeliveryNeverEmitsMarker() throws Exception {
        setup(); AtomicBoolean active = new AtomicBoolean(true);
        assertThrows(CancellationException.class, () -> PaimonStreamReadCoordinator.readPlan(source, active::get, item -> {
            items.add(item); active.set(false);
        }));
        assertEquals(1, items.size()); assertFalse(items.get(0).isFinished());
    }
    @Test void noProgressHasNoMarkerButSkippedSnapshotsHaveOne() throws Exception {
        when(source.name()).thenReturn("orders");
        TableScan.Plan plan = mock(TableScan.Plan.class); when(plan.splits()).thenReturn(Collections.emptyList()); when(source.plan()).thenReturn(plan);
        when(source.checkpoint()).thenReturn(1L);
        assertFalse(PaimonStreamReadCoordinator.readPlan(source, () -> true, items::add)); assertTrue(items.isEmpty());
        when(source.checkpoint()).thenReturn(1L, 3L);
        assertTrue(PaimonStreamReadCoordinator.readPlan(source, () -> true, items::add));
        assertEquals(3, items.get(0).after);
    }
}
