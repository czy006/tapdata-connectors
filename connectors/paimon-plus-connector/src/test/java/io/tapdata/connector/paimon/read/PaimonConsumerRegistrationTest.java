package io.tapdata.connector.paimon.read;

import org.apache.paimon.catalog.*;
import org.apache.paimon.schema.*;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class PaimonConsumerRegistrationTest {
    @Test void fixedStartUsesUniqueAttemptAndNeverRecreatesLostProtection() throws Exception {
        Path root=Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")),"consumer-");
        try(Catalog catalog=CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())))) {
            catalog.createDatabase("test",true);Identifier id=Identifier.create("test","orders");
            catalog.createTable(id,Schema.newBuilder().column("id",DataTypes.INT()).option("bucket","-1").option("consumer.expiration-time","10 min").build(),false);
            FileStoreTable table=(FileStoreTable)catalog.getTable(id);
            PaimonReadPositionIntegrationTest.write(table,1);
            PaimonReadOffset offset=new PaimonReadOffset(UUID.randomUUID().toString(),Collections.singletonMap("orders",new PaimonReadOffset.Position(PaimonReadPositionResolver.identity(table),1,2)));
            PaimonConsumerRegistration first=new PaimonConsumerRegistration(table,"orders",offset,UUID.randomUUID().toString(),()->true);
            PaimonConsumerRegistration second=new PaimonConsumerRegistration(table,"orders",offset,UUID.randomUUID().toString(),()->true);
            assertNotEquals(first.id(),second.id());assertEquals(java.time.Duration.ofMinutes(10).toNanos(),first.readTtlNanos());
            first.writeAndVerify();second.readTtlNanos();second.writeAndVerify();first.writeAndVerify();
            assertEquals(1,table.consumerManager().consumer(first.id()).get().nextSnapshot());
            table.consumerManager().deleteConsumer(first.id());
            assertThrows(IllegalStateException.class,first::writeAndVerify);
            assertFalse(table.consumerManager().consumer(first.id()).isPresent());
            catalog.alterTable(id,Collections.singletonList(SchemaChange.setOption("consumer.expiration-time","11 min")),false);
            assertThrows(IllegalStateException.class,second::readTtlNanos);
        }
    }
    @Test void missingAndShortPersistedTtlFailEvenIfDynamicCopyProvidesIt() throws Exception {
        Path root=Files.createTempDirectory(Files.createDirectories(Paths.get("target/stream-read-validation")),"ttl-");
        try(Catalog catalog=CatalogFactory.createCatalog(CatalogContext.create(new org.apache.paimon.fs.Path(root.toUri())))) {
            catalog.createDatabase("test",true);Identifier id=Identifier.create("test","orders");
            catalog.createTable(id,Schema.newBuilder().column("id",DataTypes.INT()).option("bucket","-1").build(),false);
            FileStoreTable table=((FileStoreTable)catalog.getTable(id)).copy(Collections.singletonMap("consumer.expiration-time","20 min"));
            PaimonReadOffset offset=new PaimonReadOffset(UUID.randomUUID().toString(),Collections.singletonMap("orders",new PaimonReadOffset.Position(PaimonReadPositionResolver.identity(table),1,1)));
            PaimonConsumerRegistration registration=new PaimonConsumerRegistration(table,"orders",offset,UUID.randomUUID().toString(),()->true);
            assertThrows(IllegalStateException.class,registration::readTtlNanos);
            catalog.alterTable(id,Collections.singletonList(SchemaChange.setOption("consumer.expiration-time","9 min")),false);
            assertThrows(IllegalStateException.class,registration::readTtlNanos);
            catalog.alterTable(id,Collections.singletonList(SchemaChange.setOption("consumer.expiration-time","36500 d")),false);
            assertThrows(IllegalStateException.class,registration::readTtlNanos);
            catalog.alterTable(id,Collections.singletonList(SchemaChange.setOption("consumer.expiration-time","1000000000 d")),false);
            assertThrows(IllegalStateException.class,registration::readTtlNanos);
            assertTrue(table.consumerManager().listAllIds().isEmpty());
        }
    }
}
