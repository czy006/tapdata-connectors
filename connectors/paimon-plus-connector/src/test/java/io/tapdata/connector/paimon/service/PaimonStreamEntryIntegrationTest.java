package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.read.*;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.*;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(30)
class PaimonStreamEntryIntegrationTest {
    static class Fixture implements AutoCloseable {
        final PaimonConfig config = new PaimonConfig();
        final PaimonService service;
        final Catalog catalog;
        final FileStoreTable table;
        final TapConnectorContext context = mock(TapConnectorContext.class);
        Fixture() throws Exception {
            Path root = Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")), "entry-");
            catalog = CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())));
            catalog.createDatabase("test", true);
            catalog.createTable(Identifier.create("test","orders"), Schema.newBuilder().column("id", DataTypes.INT()).option("bucket","-1").build(), false);
            table = (FileStoreTable) catalog.getTable(Identifier.create("test","orders"));
            config.setDatabase("test"); config.setEnableAsyncCommit(false); config.setStreamReadPollIntervalMs(100);
            service = new PaimonService(config, mock(Log.class)); service.startForTest();
            java.lang.reflect.Field field = PaimonService.class.getDeclaredField("catalog"); field.setAccessible(true); field.set(service,catalog);
            KVReadOnlyMap<TapTable> tables = mock(KVReadOnlyMap.class);
            when(tables.get("orders")).thenReturn(new TapTable("orders").add(new TapField("id","INT")));
            when(context.getTableMap()).thenReturn(tables);
        }
        Object offset() throws Exception { return service.timestampToStreamOffset(Collections.singletonList("orders"), 0L, mock(Log.class)); }
        void write(int id) throws Exception {
            BatchWriteBuilder builder=table.newBatchWriteBuilder();
            try(BatchTableWrite writer=builder.newWrite();BatchTableCommit commit=builder.newCommit()) {
                writer.write(GenericRow.of(id)); commit.commit(writer.prepareCommit());
            }
        }
        public void close() throws Exception { service.close(); }
    }
    @Test void emptyStartThenContinuousDeliveryAndExactRecovery() throws Exception {
        try(Fixture f=new Fixture()) {
            Object initial=f.offset(); assertEquals(1,PaimonReadOffset.parse(initial).position("orders").nextSnapshot);
            AtomicBoolean running=new AtomicBoolean(true); List<Integer> rows=new CopyOnWriteArrayList<>();
            AtomicReference<Object> saved=new AtomicReference<>(); CountDownLatch progressed=new CountDownLatch(1);
            StreamReadConsumer consumer=StreamReadConsumer.create((events,offset)->{
                events.stream().filter(e->e instanceof TapInsertRecordEvent).forEach(e->rows.add((Integer)((TapInsertRecordEvent)e).getAfter().get("id")));
                if(PaimonReadOffset.parse(offset).position("orders").nextSnapshot==2) { saved.set(offset);progressed.countDown(); }
            });
            ExecutorService executor=Executors.newSingleThreadExecutor();
            try {
                Future<?> read=executor.submit(()->{f.service.streamRead(Collections.singletonList("orders"),initial,1,consumer,f.context,running::get);return null;});
                f.write(7); assertTrue(progressed.await(10,TimeUnit.SECONDS)); running.set(false);read.get(5,TimeUnit.SECONDS);
                assertEquals(Collections.singletonList(7),rows); assertEquals(StreamReadConsumer.STATE_STREAM_READ_ENDED,consumer.getState());
                assertFalse(consumer.isAsyncMethodAndNoRetry()); assertTrue(f.table.consumerManager().listAllIds().isEmpty());
                f.write(8); running.set(true); rows.clear();
                StreamReadConsumer resumed=StreamReadConsumer.create((events,offset)->{
                    events.stream().filter(e->e instanceof TapInsertRecordEvent).forEach(e->rows.add((Integer)((TapInsertRecordEvent)e).getAfter().get("id")));
                    if(PaimonReadOffset.parse(offset).position("orders").nextSnapshot==3)running.set(false);
                });
                f.service.streamRead(Collections.singletonList("orders"),saved.get(),100,resumed,f.context,running::get);
                assertEquals(Collections.singletonList(8),rows);
            } finally {running.set(false);executor.shutdownNow();assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS));}
        }
    }
    @Test void badInputsAndCallbackFailureDoNotAdvanceOrSkip() throws Exception {
        try(Fixture f=new Fixture()) {
            StreamReadConsumer consumer=StreamReadConsumer.create((e,o)->{});
            assertThrows(IllegalArgumentException.class,()->f.service.streamRead(Collections.singletonList("orders"),null,1,consumer,f.context,()->true));
            assertThrows(IllegalArgumentException.class,()->f.service.streamRead(Collections.singletonList("orders"),Collections.singletonMap("orders",1),1,consumer,f.context,()->true));
            assertThrows(Exception.class,()->f.service.timestampToStreamOffset(Arrays.asList("orders","missing"),0L,mock(Log.class)));
            assertThrows(IllegalArgumentException.class,()->f.service.timestampToStreamOffset(Arrays.asList("orders","orders"),0L,mock(Log.class)));
            Object offset=f.offset();f.write(1);
            RuntimeException failure=new RuntimeException("callback failed");
            StreamReadConsumer failing=StreamReadConsumer.create((e,o)->{throw failure;});
            assertSame(failure,assertThrows(RuntimeException.class,()->f.service.streamRead(Collections.singletonList("orders"),offset,1,failing,f.context,()->true)));
            assertEquals(StreamReadConsumer.STATE_STREAM_READ_ENDED,failing.getState());
            assertEquals(1,PaimonReadOffset.parse(offset).position("orders").nextSnapshot);
        }
    }
}
