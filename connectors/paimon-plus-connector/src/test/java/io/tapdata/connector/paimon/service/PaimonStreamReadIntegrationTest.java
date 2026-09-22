package io.tapdata.connector.paimon.service;

import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.read.*;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.dml.*;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.*;
import io.tapdata.entity.utils.cache.KVReadOnlyMap;
import io.tapdata.pdk.apis.consumer.StreamReadConsumer;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.paimon.catalog.*;
import org.apache.paimon.data.*;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.*;
import org.apache.paimon.types.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Timeout(45)
class PaimonStreamReadIntegrationTest {
    static class Fixture implements AutoCloseable {
        final Catalog catalog;final FileStoreTable table;final PaimonService service;
        final TapConnectorContext context=mock(TapConnectorContext.class);
        final Object start;
        Fixture(boolean pk,Map<String,String> options) throws Exception {
            Path root=Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")),"matrix-");
            catalog=CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())));
            catalog.createDatabase("test",true);
            Schema.Builder schema=Schema.newBuilder().column("id",DataTypes.INT().notNull()).column("v",DataTypes.STRING());
            schema.option("bucket",pk?"1":"-1");if(pk)schema.primaryKey("id");options.forEach(schema::option);
            catalog.createTable(Identifier.create("test","t"),schema.build(),false);
            table=(FileStoreTable)catalog.getTable(Identifier.create("test","t"));
            PaimonConfig config=new PaimonConfig();config.setDatabase("test");config.setEnableAsyncCommit(false);config.setStreamReadPollIntervalMs(100);
            service=new PaimonService(config,mock(Log.class));service.startForTest();
            java.lang.reflect.Field field=PaimonService.class.getDeclaredField("catalog");field.setAccessible(true);field.set(service,catalog);
            TapTable tap=new TapTable("t").add(new TapField("id","INT").nullable(false)).add(new TapField("v","STRING"));
            KVReadOnlyMap<TapTable> tables=mock(KVReadOnlyMap.class);when(tables.get("t")).thenReturn(tap);when(context.getTableMap()).thenReturn(tables);
            start=service.timestampToStreamOffset(Collections.singletonList("t"),0L,mock(Log.class));
        }
        List<String> read(Object offset,int batchSize) throws Exception {
            long end=table.snapshotManager().latestSnapshotIdFromFileSystem()+1;
            AtomicBoolean running=new AtomicBoolean(true);List<String> events=new ArrayList<>();
            service.streamRead(Collections.singletonList("t"),offset,batchSize,StreamReadConsumer.create((batch,state)->{
                for(TapEvent event:batch) {
                    if(event instanceof TapInsertRecordEvent)events.add("+"+((TapInsertRecordEvent)event).getAfter().get("v"));
                    if(event instanceof TapDeleteRecordEvent)events.add("-"+((TapDeleteRecordEvent)event).getBefore().get("v"));
                    if(event instanceof TapInsertRecordEvent)assertTrue(((TapInsertRecordEvent)event).getReferenceTime()>0);
                }
                if(PaimonReadOffset.parse(state).position("t").nextSnapshot==end)running.set(false);
            }),context,running::get);
            return events;
        }
        void batch(boolean overwrite,String... values) throws Exception {
            BatchWriteBuilder builder=table.newBatchWriteBuilder();if(overwrite)builder.withOverwrite();
            try(BatchTableWrite writer=builder.newWrite();BatchTableCommit commit=builder.newCommit()) {
                for(String v:values)writer.write(GenericRow.of(1,BinaryString.fromString(v)));
                commit.commit(writer.prepareCommit());
            }
        }
        public void close() throws Exception {service.close();}
    }
    @Test void primaryKeyProducerMatrixUsesNativeChangelogs() throws Exception {
        for(String producer:Arrays.asList("none","input","lookup","full-compaction")) {
            try(Fixture f=new Fixture(true,Collections.singletonMap("changelog-producer",producer))) {
                StreamWriteBuilder builder=f.table.newStreamWriteBuilder();
                try(org.apache.paimon.disk.IOManager io=new org.apache.paimon.disk.IOManagerImpl(
                        Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")),"lookup-").toString());
                        StreamTableWrite writer=builder.newWrite();StreamTableCommit commit=builder.newCommit()) {
                    writer.withIOManager(io);
                    writer.write(GenericRow.ofKind(RowKind.INSERT,1,BinaryString.fromString("a")));
                    if(producer.equals("full-compaction"))writer.compact(BinaryRow.EMPTY_ROW,0,true);
                    commit.commit(1,writer.prepareCommit(true,1));
                    if(producer.equals("input"))writer.write(GenericRow.ofKind(RowKind.UPDATE_BEFORE,1,BinaryString.fromString("a")));
                    writer.write(GenericRow.ofKind(RowKind.UPDATE_AFTER,1,BinaryString.fromString("b")));
                    if(producer.equals("full-compaction"))writer.compact(BinaryRow.EMPTY_ROW,0,true);
                    commit.commit(2,writer.prepareCommit(true,2));
                    writer.write(GenericRow.ofKind(RowKind.DELETE,1,BinaryString.fromString("b")));
                    if(producer.equals("full-compaction"))writer.compact(BinaryRow.EMPTY_ROW,0,true);
                    commit.commit(3,writer.prepareCommit(true,3));
                }
                List<String> expected=producer.equals("none")?Arrays.asList("+a","+b","-b"):Arrays.asList("+a","-a","+b","-b");
                assertEquals(expected,f.read(f.start,1),producer);
            }
        }
    }
    @Test void appendOverwriteDefaultsToSkipAndOptInEmitsReplacementInserts() throws Exception {
        for(boolean enabled:Arrays.asList(false,true)) {
            try(Fixture f=new Fixture(false,enabled?Collections.singletonMap("streaming-read-append-overwrite","true"):Collections.emptyMap())) {
                f.batch(false,"a");f.batch(true,"b");
                assertEquals(enabled?Arrays.asList("+a","+b"):Collections.singletonList("+a"),f.read(f.start,100));
            }
        }
    }
    @Test void primaryKeyOverwriteDefaultsToSkipAndOptInEmitsDeleteInsert() throws Exception {
        for(boolean enabled:Arrays.asList(false,true)) {
            try(Fixture f=new Fixture(true,enabled?Collections.singletonMap("streaming-read-overwrite","true"):Collections.emptyMap())) {
                f.batch(false,"a");f.batch(true,"b");
                assertEquals(enabled?Arrays.asList("+a","-a","+b"):Collections.singletonList("+a"),f.read(f.start,100));
            }
        }
    }
    @Test void expiredSnapshotsRemainReadableThroughDecoupledChangelogAndMissingHistoryFails() throws Exception {
        Map<String,String> options=new HashMap<>();
        options.put("changelog-producer","input");options.put("snapshot.num-retained.min","1");
        options.put("snapshot.num-retained.max","1");options.put("snapshot.time-retained","1 ms");
        options.put("changelog.num-retained.min","100");options.put("changelog.num-retained.max","100");
        options.put("changelog.time-retained","1 h");
        try(Fixture f=new Fixture(true,options)) {
            f.batch(false,"a");Thread.sleep(5);f.batch(false,"b");Thread.sleep(5);f.batch(false,"c");
            assertTrue(f.table.snapshotManager().earliestSnapshotId()>1);
            assertFalse(f.table.snapshotManager().snapshotExists(1));
            assertTrue(f.table.changelogManager().longLivedChangelogExists(1));
            assertEquals(Arrays.asList("+a","+b","+c"),f.read(f.start,100));
            f.table.fileIO().delete(f.table.changelogManager().longLivedChangelogPath(1),false);
            assertThrows(io.tapdata.exception.TapPdkOffsetOutOfLogEx.class,()->f.read(f.start,100));
        }
    }
    @Test void noneModeIgnoresPersistedConsumerProgressAndDoesNotOverwriteItsFile() throws Exception {
        try(Fixture f=new Fixture(false,Collections.singletonMap("consumer-id","existing"))) {
            f.table.consumerManager().resetConsumer("existing",new org.apache.paimon.consumer.Consumer(123));
            f.batch(false,"a");assertEquals(Collections.singletonList("+a"),f.read(f.start,100));
            assertEquals(123,f.table.consumerManager().consumer("existing").get().nextSnapshot());
            assertEquals(Collections.singletonList("existing"),f.table.consumerManager().listAllIds());
        }
    }
    @Test void partialPlanCallbackFailureReplaysFromUnchangedSavedOffset() throws Exception {
        try(Fixture f=new Fixture(false,Collections.emptyMap())) {
            f.batch(false,"a","b");AtomicReference<Object> persisted=new AtomicReference<>(f.start);AtomicInteger seen=new AtomicInteger();
            RuntimeException crash=new RuntimeException("crash in plan");
            assertSame(crash,assertThrows(RuntimeException.class,()->f.service.streamRead(Collections.singletonList("t"),f.start,1,
                StreamReadConsumer.create((events,state)->{persisted.set(state);if(seen.incrementAndGet()==1)throw crash;}),f.context,()->true)));
            assertEquals(1,PaimonReadOffset.parse(persisted.get()).position("t").nextSnapshot);
            assertEquals(Arrays.asList("+a","+b"),f.read(persisted.get(),100));
        }
    }
}
