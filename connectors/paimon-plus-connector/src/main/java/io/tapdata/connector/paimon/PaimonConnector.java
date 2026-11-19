package io.tapdata.connector.paimon;

import io.tapdata.base.ConnectorBase;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.connector.paimon.recovery.PreparedMessageStore;
import io.tapdata.connector.paimon.exception.PaimonFatalWriteException;
import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.connector.paimon.schema.PaimonRecoveryTime;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.TapEvent;
import io.tapdata.entity.event.control.ControlEvent;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.event.ddl.index.TapCreateIndexEvent;
import io.tapdata.entity.event.ddl.table.TapCreateTableEvent;
import io.tapdata.entity.event.ddl.table.TapDropTableEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.schema.value.*;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.entity.utils.cache.KVMap;
import io.tapdata.entity.utils.cache.Entry;
import io.tapdata.entity.utils.cache.Iterator;
import io.tapdata.pdk.apis.annotations.TapConnectorClass;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.ConnectionOptions;
import io.tapdata.pdk.apis.entity.ExecuteResult;
import io.tapdata.pdk.apis.entity.TestItem;
import io.tapdata.pdk.apis.entity.TapExecuteCommand;
import io.tapdata.pdk.apis.entity.WriteListResult;
import io.tapdata.pdk.apis.entity.TapAdvanceFilter;
import io.tapdata.pdk.apis.entity.FilterResults;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import io.tapdata.pdk.apis.functions.connector.target.CreateTableOptions;
import org.apache.commons.collections4.MapUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;

/**
 * Paimon Connector for Tapdata
 * Supports write operations to Apache Paimon tables
 *
 * @author Tapdata
 */
@TapConnectorClass("spec.json")
public class PaimonConnector extends ConnectorBase {

    private static final String TAG = PaimonConnector.class.getSimpleName();
    private static final Pattern CALENDAR_DATE_TEXT =
            Pattern.compile("(?:\\d{8}|\\d{4}-\\d{2}-\\d{2}"
                    + "(?:[Tt]\\d{2}:\\d{2}(?::\\d{2}(?:\\.\\d{1,9})?)?"
                    + "(?:[Zz]|[+-]\\d{2}:\\d{2}(?::\\d{2})?)?)?)");
    
    private volatile PaimonService paimonService;
    private volatile RecoverySession recoverySession;

    private static final class RecoverySession {
        final TapConnectorContext context;
        final PaimonService service;
        final boolean localRecoveryEnabled;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        volatile boolean ready;
        volatile boolean stopping;

        RecoverySession(TapConnectorContext context, PaimonService service, boolean localRecoveryEnabled) {
            this.context = context;
            this.service = service;
            this.localRecoveryEnabled = localRecoveryEnabled;
        }
    }

    /**
     * Initialize connection when connector starts
     *
     * @param connectionContext connection context
     * @throws Throwable if initialization fails
     */
    @Override
    public void onStart(TapConnectionContext connectionContext) throws Throwable {
        start(connectionContext, connectionContext instanceof TapConnectorContext);
    }

    @Override
    public void onLightStart(TapConnectionContext connectionContext) throws Throwable {
        start(connectionContext, false);
    }

    private static PaimonConfig configuration(TapConnectionContext context, boolean task) {
        if (MapUtils.isEmpty(context.getConnectionConfig())) {
            throw new IllegalArgumentException("Connection config cannot be empty");
        }
        PaimonConfig config = new PaimonConfig().load(context.getConnectionConfig());
        if (MapUtils.isNotEmpty(context.getNodeConfig())) {
            DataMap node = DataMap.create(context.getNodeConfig());
            node.remove("database");
            if (task) {
                config.loadNodeConfig(node);
            } else {
                config.load(node);
            }
        }
        if (task || MapUtils.isNotEmpty(context.getNodeConfig())) {
            config.setTableConfig(context.getTableNodeConfig());
        }
        return config;
    }

    private void start(TapConnectionContext context, boolean task) throws Throwable {
        PaimonConfig config = configuration(context, task);
        boolean recovery = task && config.isLocalRecoveryEnabled();
        Consumer<Object> callback = context.getFlushOffsetCallback();
        // HazelcastTargetPdkBaseNode#createPdkAndInit installs this sink callback before onStart.
        if (recovery && callback == null) {
            throw new IllegalStateException("Local recovery requires an active FLUSH_OFFSET_CALLBACK target");
        }
        KVMap<Object> stateMap = task ? ((TapConnectorContext) context).getStateMap() : null;
        PaimonService service = new PaimonService(config, context.getLog());
        // A formerly bound target can lose both the safety option and the sink callback.
        // An ordinary source has its own node KV and only reads this key.
        RecoverySession session = task && (callback != null || stateMap != null)
                ? new RecoverySession((TapConnectorContext) context, service, recovery) : null;
        synchronized (this) {
            if (paimonService != null) {
                throw new IllegalStateException("Paimon connector already started");
            }
            recoverySession = session;
            paimonService = service;
        }
        try {
            if (session != null && !recovery) {
                if (stateMap == null) {
                    throw new IllegalStateException("Paimon target needs its node state map to check for an existing local recovery binding");
                }
                if (PreparedMessageStore.hasBinding(stateMap)) {
                    throw new IllegalStateException("This target node is bound to local recovery. Restore its original localRecoveryDir.");
                }
            }
            service.setFlushOffsetCallback(callback);
            if (recovery) {
                service.setRecoveryFailureListener(failure -> reportRecoveryFailure(session, session.context, failure));
            }
            service.init();
            if (session != null) {
                if (recovery) {
                    service.startLocalRecovery(session.context);
                    // TapData 65add935fdee: readNormalAndLogCollectorTaskStreamOffset may take the
                    // current position after an older saveToSnapshot overwrites a newer progress record.
                    // The PDK cannot verify which position the source actually resumed from.
                    // https://github.com/tapdata/tapdata/blob/65add935fdee/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkBaseNode.java#L792-L800
                    service.requireFreshSourceSession();
                }
                synchronized (this) {
                    if (paimonService != service || session.stopping) {
                        throw new IllegalStateException("Paimon connector stopped during recovery");
                    }
                    if (session.failure.get() != null) {
                        throw session.failure.get();
                    }
                    session.ready = true;
                    if (!recovery) {
                        recoverySession = null;
                    }
                }
            }
            context.getLog().info("Paimon connector started successfully");
        } catch (Throwable failure) {
            if (session != null) {
                session.failure.compareAndSet(null, failure);
                session.ready = false;
            }
            boolean ownsService;
            synchronized (this) {
                ownsService = paimonService == service && (session == null || !session.stopping);
                if (ownsService) {
                    paimonService = null;
                }
            }
            if (ownsService) {
                try {
                    service.close();
                } catch (Throwable closeFailure) {
                    if (failure != closeFailure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            throw failure;
        }
    }

    /**
     * Clean up resources when connector stops
     *
     * @param connectionContext connection context
    */
    @Override
    public void onStop(TapConnectionContext connectionContext) throws Throwable {
        PaimonService service;
        RecoverySession session;
        synchronized (this) {
            session = recoverySession;
            if (session != null && session.context != connectionContext) {
                return;
            }
            service = paimonService;
            if (session == null) {
                paimonService = null;
            } else {
                session.stopping = true;
                session.ready = false;
            }
        }
        if (service != null) {
            try {
                service.close();
            } catch (Exception e) {
                io.tapdata.connector.paimon.service.PaimonStopLog.offer(connectionContext.getLog(),
                        "[paimon-stop] event=connector-stop-failed", e);
                throw e;
            } finally {
                synchronized (this) {
                    if (paimonService == service) {
                        paimonService = null;
                    }
                }
            }
        }
    }

    /**
     * Test connection to Paimon
     *
     * @param connectionContext connection context
     * @param consumer test result consumer
     * @return connection options
     * @throws Throwable if test fails
     */
    @Override
    public ConnectionOptions connectionTest(TapConnectionContext connectionContext, Consumer<TestItem> consumer) throws Throwable {
        ConnectionOptions connectionOptions = ConnectionOptions.create();
        PaimonService probe = null;
        try {
            PaimonConfig config = configuration(connectionContext, false);
            probe = new PaimonService(config, connectionContext.getLog());
            probe.init();
            connectionOptions.connectionString(config.getConnectionString());
            // Test warehouse accessibility
            boolean warehouseAccessible = probe.testWarehouseAccess();
            if (warehouseAccessible) {
                consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_SUCCESSFULLY, 
                    "Warehouse is accessible"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, 
                    "Cannot access warehouse"));
                return connectionOptions;
            }
            
            // Test write permission
            boolean canWrite = probe.testWritePermission();
            if (canWrite) {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_SUCCESSFULLY, 
                    "Write permission verified"));
            } else {
                consumer.accept(testItem(TestItem.ITEM_WRITE, TestItem.RESULT_FAILED, 
                    "No write permission"));
            }
            
        } catch (Throwable throwable) {
            consumer.accept(testItem(TestItem.ITEM_CONNECTION, TestItem.RESULT_FAILED, 
                "Connection test failed: " + throwable.getMessage()));
            throw throwable;
        } finally {
            if (probe != null) {
                probe.close();
            }
        }
        
        return connectionOptions;
    }

    @Override
    public int tableCount(TapConnectionContext connectionContext) throws Throwable {
        return paimonService.getTableCount();
    }

    /**
     * Discover schema - load table definitions from Paimon
     *
     * @param connectionContext connection context
     * @param tables list of table names to discover (null for all tables)
     * @param tableSize batch size for table discovery
     * @param consumer consumer for discovered tables
     * @throws Throwable if discovery fails
     */
    @Override
    public void discoverSchema(TapConnectionContext connectionContext, List<String> tables, 
                               int tableSize, Consumer<List<TapTable>> consumer) throws Throwable {
        List<TapTable> tapTables = paimonService.discoverTables(tables);
        consumer.accept(tapTables);
    }

    /**
     * Register connector capabilities
     *
     * @param connectorFunctions connector functions registry
     * @param codecRegistry codec registry
     */
    @Override
    public void registerCapabilities(ConnectorFunctions connectorFunctions, TapCodecsRegistry codecRegistry) {
        // Source capabilities
        connectorFunctions.supportBatchRead(this::batchRead);
        connectorFunctions.supportBatchCount(this::batchCount);
        connectorFunctions.supportStreamRead(this::streamRead);
        connectorFunctions.supportTimestampToStreamOffset(this::timestampToStreamOffset);
        connectorFunctions.supportQueryByAdvanceFilter(this::queryByAdvanceFilter);
        connectorFunctions.supportExecuteCommandFunction(this::executeCommand);

        // Target capabilities
        connectorFunctions.supportWriteRecord(this::writeRecord);
        connectorFunctions.supportCreateTableV2(this::createTable);
        connectorFunctions.supportDropTable(this::dropTable);
        connectorFunctions.supportCreateIndex(this::createIndex);
        connectorFunctions.supportClearTable(this::clearTable);
        connectorFunctions.supportProcessControlFunction(this::processControl);
        connectorFunctions.supportAfterInitialSync(this::afterInitialSync);

        // Register codec for data type conversions
        registerCodecs(codecRegistry);
    }

    /**
     * Register data type codecs
     *
     * @param codecRegistry codec registry
     */
    private void registerCodecs(TapCodecsRegistry codecRegistry) {
        // Register codec for TapRawValue - convert to JSON string
        codecRegistry.registerFromTapValue(TapRawValue.class, "STRING", tapRawValue -> {
            if (tapRawValue != null && tapRawValue.getValue() != null) {
                return toJson(tapRawValue.getValue());
            }
            return null;
        });

        // Register codec for TapMapValue - convert to JSON string
        codecRegistry.registerFromTapValue(TapMapValue.class, "STRING", tapMapValue -> {
            if (tapMapValue != null && tapMapValue.getValue() != null) {
                return toJson(tapMapValue.getValue());
            }
            return null;
        });

        // Register codec for TapArrayValue - convert to JSON string
        codecRegistry.registerFromTapValue(TapArrayValue.class, "STRING", tapArrayValue -> {
            if (tapArrayValue != null && tapArrayValue.getValue() != null) {
                return toJson(tapArrayValue.getValue());
            }
            return null;
        });
        codecRegistry.registerFromTapValue(
                TapDateTimeValue.class,
                t -> {
                    if (t == null || null == t.getValue() ||  t.getValue().isContainsIllegal()) {
                        return null;
                    }
                    return t.getValue().toTimestamp();
                });
        codecRegistry.registerFromTapValue(TapDateValue.class, t -> {
            if (t == null || null == t.getValue() ||  t.getValue().isContainsIllegal()) {
                return null;
            }
            return PaimonConnector.toEpochDay(t);
        });
        codecRegistry.registerFromTapValue(TapTimeValue.class, t -> {
            if (t == null || null == t.getValue() ||  t.getValue().isContainsIllegal()) {
                return null;
            }
            // The active node registry is shared by the engine's full-load and CDC filters.
            // Preserve precision until the writer can distinguish a business column from a key.
            LocalTime time = toLocalTime(t);
            RecoverySession session = recoverySession;
            if (session != null && session.localRecoveryEnabled) {
                return new PaimonRecoveryTime(time);
            }
            return Math.toIntExact(time.toNanoOfDay() / 1_000_000L);
        });
        codecRegistry.registerFromTapValue(TapYearValue.class, "CHAR(4)", PaimonConnector::toYear);
    }

    private static String toYear(TapYearValue value) {
        if (value == null || value.getValue() == null || value.getValue().isContainsIllegal()) {
            return null;
        }
        Object origin = value.getOriginValue();
        try {
            // Connector/J exposes YEAR as a SQL date; CDC exposes it as an integer.
            int year = origin instanceof java.sql.Date
                    ? ((java.sql.Date) origin).toLocalDate().getYear()
                    : origin instanceof Number || origin instanceof CharSequence
                            ? new BigDecimal(origin.toString()).intValueExact()
                            : value.getValue().toLocalDateTime().getYear();
            if (year < 0 || year > 9999) {
                throw new ArithmeticException("year is outside CHAR(4)");
            }
            return String.format(Locale.ROOT, "%04d", year);
        } catch (RuntimeException e) {
            throw temporalConversionFailure(
                    "TapYear", "CHAR(4)", origin, "value must be an integer year from 0000 to 9999");
        }
    }

    private static Integer toEpochDay(TapDateValue tapDateValue) {
        if (tapDateValue == null || tapDateValue.getValue() == null) {
            return null;
        }
        Object origin = tapDateValue.getOriginValue();
        // SQL Date and date text carry a calendar date, not a UTC instant.
        if (origin instanceof java.sql.Date) {
            return Math.toIntExact(((java.sql.Date) origin).toLocalDate().toEpochDay());
        }
        if (origin instanceof CharSequence) {
            String text = origin.toString().trim().replace('/', '-').replace(' ', 'T');
            if (CALENDAR_DATE_TEXT.matcher(text).matches()) {
                DateTimeFormatter formatter = text.length() == 8 ? DateTimeFormatter.BASIC_ISO_DATE
                        : text.length() == 10 ? DateTimeFormatter.ISO_LOCAL_DATE
                        : DateTimeFormatter.ISO_DATE_TIME;
                try {
                    LocalDate date = LocalDate.parse(text, formatter);
                    return date.getYear() == 0 ? null : Math.toIntExact(date.toEpochDay());
                } catch (DateTimeParseException e) {
                    // Zero dates and invalid calendar/time fields cannot be represented by DATE.
                    return null;
                }
            }
            // Custom source formats may already have a valid normalized DateTime.
            // Preserve the existing fallback instead of rejecting their original text.
        }
        Long seconds = tapDateValue.getValue().getSeconds();
        if (seconds == null) {
            throw temporalConversionFailure(
                    "TapDate", "DATE", tapDateValue.getValue(), "epoch seconds are missing");
        }
        try {
            return Math.toIntExact(Math.floorDiv(seconds, 86_400L));
        } catch (ArithmeticException e) {
            throw temporalConversionFailure(
                    "TapDate", "DATE", tapDateValue.getValue(), "value is outside the DATE range");
        }
    }

    private static LocalTime toLocalTime(TapTimeValue tapTimeValue) {
        Long seconds = tapTimeValue.getValue().getSeconds();
        Integer nanos = tapTimeValue.getValue().getNano();
        if (seconds == null || nanos == null) {
            throw temporalConversionFailure(
                    "TapTime",
                    "TIME(3)",
                    tapTimeValue.getValue(),
                    "seconds or nanoseconds are missing");
        }
        if (seconds < 0 || seconds >= 86_400L || nanos < 0 || nanos >= 1_000_000_000) {
            throw temporalConversionFailure(
                    "TapTime",
                    "TIME(3)",
                    tapTimeValue.getValue(),
                    "value must be within one day");
        }
        return LocalTime.ofSecondOfDay(seconds).withNano(nanos);
    }

    private static PaimonFatalWriteException temporalConversionFailure(
            String codecField, String targetType, Object source, String reason) {
        return new PaimonFatalWriteException(
                "PAIMON_VALUE_CONVERSION_FAILED: field=<codec:"
                        + codecField
                        + ">, target="
                        + targetType
                        + ", source="
                        + (source == null ? "null" : source.getClass().getName())
                        + ", reason="
                        + reason);
    }

    /**
     * Create table in Paimon
     *
     * @param connectorContext connector context
     * @param createTableEvent create table event
     * @return create table options
     * @throws Throwable if creation fails
     */
    private CreateTableOptions createTable(TapConnectorContext connectorContext,
                                           TapCreateTableEvent createTableEvent) throws Throwable {
        final Log log = connectorContext.getLog();
        CreateTableOptions createTableOptions = new CreateTableOptions();
        createTableOptions.setTableExists(false);
        
        TapTable table = createTableEvent.getTable();
        log.info("Creating Paimon table: " + table.getName());
        
        boolean created = targetService(connectorContext).createTable(table);
        if (created) {
            log.info("Table created successfully: " + table.getName());
        } else {
            createTableOptions.setTableExists(true);
            log.info("Table already exists: " + table.getName());
        }
        
        return createTableOptions;
    }

    /**
     * Drop table from Paimon
     *
     * @param connectorContext connector context
     * @param dropTableEvent drop table event
     * @throws Throwable if drop fails
     */
    private void dropTable(TapConnectorContext connectorContext, TapDropTableEvent dropTableEvent) throws Throwable {
        final Log log = connectorContext.getLog();
        String tableName = dropTableEvent.getTableId();
        
        log.info("Dropping Paimon table: " + tableName);
        targetService(connectorContext).dropTable(tableName);
        log.info("Table dropped successfully: " + tableName);
    }

    /**
     * Clear all data from table
     *
     * @param connectorContext connector context
     * @param tapClearTableEvent clear table event
     * @throws Throwable if clear fails
     */
    private void clearTable(TapConnectorContext connectorContext, 
                           io.tapdata.entity.event.ddl.table.TapClearTableEvent tapClearTableEvent) throws Throwable {
        final Log log = connectorContext.getLog();
        String tableName = tapClearTableEvent.getTableId();
        
        log.info("Clearing Paimon table: " + tableName);
        targetService(connectorContext).clearTable(tableName);
        log.info("Table cleared successfully: " + tableName);
    }

    /**
     * Create index on Paimon table
     *
     * @param connectorContext connector context
     * @param table table definition
     * @param createIndexEvent create index event
     */
    private void createIndex(TapConnectorContext connectorContext, TapTable table, 
                            TapCreateIndexEvent createIndexEvent) {
        final Log log = connectorContext.getLog();
        
        PaimonService service = targetService(connectorContext);
        log.info("Creating index on table: " + table.getName());
        try {
            service.createIndex(table, createIndexEvent.getIndexList());
            log.info("Index created successfully on table: " + table.getName());
        } catch (Exception e) {
            log.warn("Failed to create index on table " + table.getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Batch read records from Paimon table
     *
     * @param connectorContext connector context
     * @param table table to read from
     * @param offsetState offset state for resuming read
     * @param eventBatchSize batch size for events
     * @param eventsOffsetConsumer consumer for events and offset
     * @throws Throwable if read fails
     */
    private void batchRead(TapConnectorContext connectorContext, TapTable table, Object offsetState,
                          int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Starting batch read from table: " + table.getName());

            // Read records using Paimon service
            paimonService.batchRead(table, offsetState, eventBatchSize, eventsOffsetConsumer, connectorContext);

            log.info("Batch read completed for table: " + table.getName());

        } catch (Exception e) {
            log.error("Error reading records from table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Stream read records from Paimon table (CDC mode)
     *
     * @param connectorContext connector context
     * @param tables list of tables to read from
     * @param offsetState offset state for resuming read
     * @param eventBatchSize batch size for events
     * @param eventsOffsetConsumer consumer for events and offset
     * @throws Throwable if read fails
     */
    private void streamRead(TapConnectorContext connectorContext, List<String> tables, Object offsetState,
                           int eventBatchSize, BiConsumer<List<TapEvent>, Object> eventsOffsetConsumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Starting stream read from tables: " + tables);

            // Stream read records using Paimon service
            paimonService.streamRead(tables, offsetState, eventBatchSize, eventsOffsetConsumer, connectorContext, this::isAlive);

            log.info("Stream read completed for tables: " + tables);

        } catch (Exception e) {
            log.error("Error in stream read from tables " + tables + ": " + e.getMessage(), e);
            throw e;
        }
    }

    protected void afterInitialSync(TapConnectorContext connectorContext, TapTable tapTable) throws Throwable {
        if (paimonService != null || recoveryRequested(connectorContext)) {
            targetService(connectorContext).afterInitialSync(connectorContext, tapTable);
        }
    }

    /**
     * Write records to Paimon table
     *
     * @param connectorContext connector context
     * @param tapRecordEvents list of record events to write
     * @param table target table
     * @param writeListResultConsumer consumer for write results
     * @throws Throwable if write fails
     */
    private void writeRecord(TapConnectorContext connectorContext, List<TapRecordEvent> tapRecordEvents,
                            TapTable table, Consumer<WriteListResult<TapRecordEvent>> writeListResultConsumer) throws Throwable {
        final Log log = connectorContext.getLog();

        RecoverySession session = recoverySession;
        try {
            // Write records using Paimon service
            WriteListResult<TapRecordEvent> result = targetService(connectorContext).writeRecords(
                tapRecordEvents, table, connectorContext);

            writeListResultConsumer.accept(result);

        } catch (Exception e) {
            if (session == null) {
                log.error("Error writing records to table " + table.getName() + ": " + e.getMessage(), e);
            } else {
                reportRecoveryFailure(session, connectorContext, e);
            }
            throw e;
        }
    }

    /**
     * Convert timestamp to stream offset (snapshot ID) for each table
     * This allows resuming stream read from a specific point in time
     *
     * @param connectorContext connector context
     * @param timestamp timestamp in milliseconds (null for current time)
     * @return offset object containing snapshot IDs for each table
     * @throws Throwable if conversion fails
     */
    private Object timestampToStreamOffset(TapConnectorContext connectorContext, Long timestamp) throws Throwable {
        final Log log = connectorContext.getLog();

        // Build table list from context
        List<String> tableList = new ArrayList<>();
        Iterator<Entry<TapTable>> iterator = connectorContext.getTableMap().iterator();
        while (iterator.hasNext()) {
            tableList.add(iterator.next().getKey());
        }

        try {
            // Use current time if timestamp is null
            Long effectiveTimestamp = timestamp != null ? timestamp : System.currentTimeMillis();
            log.info("Converting timestamp {} to stream offset for {} tables", effectiveTimestamp, tableList.size());

            // Get snapshot IDs for specified tables at the given timestamp
            return paimonService.timestampToStreamOffset(tableList, effectiveTimestamp, log);

        } catch (Exception e) {
            log.error("Error converting timestamp to stream offset: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Count records in a table
     *
     * @param connectorContext connector context
     * @param table table to count
     * @return record count
     * @throws Throwable if count fails
     */
    private long batchCount(TapConnectorContext connectorContext, TapTable table) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Counting records in table: " + table.getName());

            // Count records using Paimon service
            long count = paimonService.batchCount(table, log);

            log.info("Table {} has {} records", table.getName(), count);
            return count;

        } catch (Exception e) {
            log.error("Error counting records in table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Query records by advance filter
     *
     * @param connectorContext connector context
     * @param filter advance filter with conditions
     * @param table table to query
     * @param consumer consumer for filter results
     * @throws Throwable if query fails
     */
    private void queryByAdvanceFilter(TapConnectorContext connectorContext, TapAdvanceFilter filter,
                                     TapTable table, Consumer<FilterResults> consumer) throws Throwable {
        final Log log = connectorContext.getLog();

        try {
            log.info("Querying table {} with advance filter", table.getName());

            // Query records using Paimon service
            paimonService.queryByAdvanceFilter(table, filter, consumer, log);

            log.info("Query completed for table: " + table.getName());

        } catch (Exception e) {
            log.error("Error querying table " + table.getName() + ": " + e.getMessage(), e);
            throw e;
        }
    }

    private void executeCommand(TapConnectorContext connectorContext, TapExecuteCommand command,
                                Consumer<ExecuteResult> consumer) throws Throwable {
        try {
            if (command == null || !"executeQuery".equals(command.getCommand())) {
                throw new IllegalArgumentException("Not supported command: "
                        + (command == null ? null : command.getCommand()));
            }
            Map<String, Object> params = command.getParams();
            Object sql = params == null ? null : params.get("sql");
            if (!(sql instanceof String) || ((String) sql).trim().isEmpty()) {
                throw new IllegalArgumentException("executeQuery requires a non-empty params.sql");
            }
            Object requestedBatchSize = params == null ? null : params.get("batchSize");
            int batchSize = requestedBatchSize == null
                    ? (command.getBatchSize() == null ? 1000 : command.getBatchSize())
                    : parseBatchSize(requestedBatchSize);
            if (batchSize <= 0) batchSize = 1000;
            paimonService.executeQueryBatches((String) sql, params, connectorContext.getLog(),
                    result -> consumer.accept(new ExecuteResult<>().result(result)), batchSize);
        } catch (Throwable throwable) {
            consumer.accept(new ExecuteResult<>().error(throwable));
        }
    }

    private static int parseBatchSize(Object value) {
        try {
            return new java.math.BigDecimal(value.toString()).intValueExact();
        } catch (NumberFormatException | ArithmeticException failure) {
            throw new IllegalArgumentException("Parameter 'batchSize' must be a whole number within the int range; "
                    + "positive values must not exceed " + Integer.MAX_VALUE + "; got: " + value, failure);
        }
    }

    protected void processControl(TapConnectorContext tapConnectorContext, ControlEvent controlEvent)
            throws Throwable {
        if (!(controlEvent instanceof HeartbeatEvent)) {
            return;
        }
        RecoverySession session = recoverySession;
        if (session == null && !recoveryRequested(tapConnectorContext)) {
            targetService(tapConnectorContext).processHeartbeat((HeartbeatEvent) controlEvent);
            return;
        }
        // The engine falls back to saving the incoming heartbeat when processControl throws
        // (TapData 65add935fdee, HazelcastTargetPdkBaseNode#flushSyncProgressMap).
        // An early or late heartbeat can be redelivered; no callback may run outside this session.
        if (session == null || session.context != tapConnectorContext || !session.ready || session.failure.get() != null) {
            return;
        }
        try {
            session.service.processHeartbeat((HeartbeatEvent) controlEvent);
        } catch (Throwable failure) {
            reportRecoveryFailure(session, tapConnectorContext, failure);
        }
    }

    private boolean recoveryRequested(TapConnectorContext context) {
        if (recoverySession != null) {
            return true;
        }
        DataMap node = context == null ? null : context.getNodeConfig();
        Object location = node == null ? null : node.get("localRecoveryDir");
        return location instanceof String && !((String) location).trim().isEmpty();
    }

    private PaimonService targetService(TapConnectorContext context) {
        RecoverySession session = recoverySession;
        PaimonService service = paimonService;
        if (session != null) {
            if (session.context != context) {
                throw new IllegalStateException("Local recovery target belongs to another task context");
            }
            Throwable failure = session.failure.get();
            if (failure != null) {
                throw new IllegalStateException("Local recovery target has failed", failure);
            }
            if (!session.ready || session.service != service) {
                throw new IllegalStateException("Local recovery target is not ready");
            }
        } else if (recoveryRequested(context)) {
            throw new IllegalStateException("Local recovery target has not been initialized");
        }
        if (service == null) {
            throw new IllegalStateException("Paimon target service is unavailable");
        }
        return service;
    }

    private static void reportRecoveryFailure(
            RecoverySession session, TapConnectorContext context, Throwable failure) {
        if (session.context != context || session.stopping || !session.failure.compareAndSet(null, failure)) {
            return;
        }
        try {
            session.service.failLocalRecovery(failure);
        } catch (Throwable fenceFailure) {
            if (failure != fenceFailure) {
                failure.addSuppressed(fenceFailure);
            }
        }
        try {
            context.getLog().error("Paimon local recovery target failed", failure);
        } catch (Throwable reportFailure) {
            if (failure != reportFailure) {
                failure.addSuppressed(reportFailure);
            }
        }
    }
}
