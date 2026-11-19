package io.tapdata.connector.paimon;

import io.tapdata.connector.paimon.service.PaimonService;
import io.tapdata.connector.paimon.config.PaimonConfig;
import io.tapdata.entity.codec.TapCodecsRegistry;
import io.tapdata.entity.event.control.HeartbeatEvent;
import io.tapdata.entity.logger.Log;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectionContext;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.functions.ConnectorFunctions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

class PaimonConnectorControlTest {

    @Test
    @SuppressWarnings("unchecked")
    void onStartMustBindFlushCallbackBeforeServiceInitialization() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        TapConnectionContext context = mock(TapConnectionContext.class);
        Consumer<Object> callback = mock(Consumer.class);
        Log log = mock(Log.class);
        when(context.getConnectionConfig())
                .thenReturn(DataMap.create().kv("warehouse", "/tmp/paimon-test"));
        when(context.getNodeConfig()).thenReturn(DataMap.create());
        when(context.getLog()).thenReturn(log);
        when(context.getFlushOffsetCallback()).thenReturn(callback);

        try (MockedConstruction<PaimonConfig> ignored =
                        mockConstruction(
                                PaimonConfig.class,
                                (config, constructionContext) ->
                                        when(config.load(anyMap())).thenReturn(config));
                MockedConstruction<PaimonService> construction =
                        mockConstruction(PaimonService.class)) {
            connector.onStart(context);

            PaimonService service = construction.constructed().get(0);
            InOrder order = inOrder(service);
            order.verify(service).setFlushOffsetCallback(callback);
            order.verify(service).init();
        }
    }

    @Test
    void heartbeatMustBeForwardedToService() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        PaimonService service = mock(PaimonService.class);
        setService(connector, service);
        HeartbeatEvent heartbeat = new HeartbeatEvent().init();

        connector.processControl(mock(TapConnectorContext.class), heartbeat);

        verify(service).processHeartbeat(heartbeat);
    }

    @Test
    void heartbeatFailureMustPropagateWithoutBeingReportedAsSuccess() throws Exception {
        PaimonConnector connector = new PaimonConnector();
        PaimonService service = mock(PaimonService.class);
        setService(connector, service);
        HeartbeatEvent heartbeat = new HeartbeatEvent().init();
        IOException failure = new IOException("heartbeat callback failed");
        doThrow(failure).when(service).processHeartbeat(heartbeat);

        Throwable thrown =
                assertThrows(
                        Throwable.class,
                        () -> connector.processControl(mock(TapConnectorContext.class), heartbeat));

        assertSame(failure, thrown);
    }

    @Test
    void recoveryHoldsCallbacksDuringStartAndStopAndReportsHeartbeatFailureOnce() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        TapConnectorContext context = recoveryContext();
        when(context.getFlushOffsetCallback()).thenReturn(value -> {});
        HeartbeatEvent heartbeat = new HeartbeatEvent().init();
        IOException failure = new IOException("failed to publish dependent table");
        try (MockedConstruction<PaimonConfig> ignored = recoveryConfig();
                MockedConstruction<PaimonService> services = mockConstruction(
                        PaimonService.class, (service, construction) -> {
                            doAnswer(inv -> {
                                connector.processControl(context, heartbeat);
                                return null;
                            }).when(service).startLocalRecovery(context);
                            doThrow(failure).when(service).processHeartbeat(heartbeat);
                        })) {
            connector.onStart(context);
            PaimonService service = services.constructed().get(0);
            verify(service).startLocalRecovery(context);
            verify(service, never()).processHeartbeat(heartbeat);
            Log log = context.getLog();
            doThrow(new IllegalStateException("engine logger unavailable")).when(log)
                    .error("Paimon local recovery target failed", failure);
            assertDoesNotThrow(() -> connector.processControl(context, heartbeat));
            assertDoesNotThrow(() -> connector.processControl(context, heartbeat));
            verify(service).failLocalRecovery(failure);
            verify(log).error("Paimon local recovery target failed", failure);
            assertSame(failure, assertThrows(IllegalStateException.class,
                    () -> connector.afterInitialSync(context, null)).getCause());
            connector.onStop(context);
            assertDoesNotThrow(() -> connector.processControl(context, heartbeat));
            verify(service).processHeartbeat(heartbeat);
        }
    }

    @Test
    void requestingRecoveryWithoutAnInstalledOffsetConsumerFailsBeforeClaimingAnyFiles() throws Exception {
        PaimonConnector connector = new PaimonConnector();
        TapConnectorContext context = recoveryContext();
        try (MockedConstruction<PaimonConfig> ignored = recoveryConfig();
                MockedConstruction<PaimonService> services = mockConstruction(PaimonService.class)) {
            assertThrows(IllegalStateException.class, () -> connector.onStart(context));
            assertTrue(services.constructed().isEmpty());
            assertDoesNotThrow(() -> connector.processControl(context, new HeartbeatEvent().init()));
        }
    }

    @Test
    void connectionTestWithTaskConfigNeverClaimsRecoveryDirectory() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        TapConnectorContext context = recoveryContext();
        try (MockedConstruction<PaimonConfig> ignored = recoveryConfig();
                MockedConstruction<PaimonService> services = mockConstruction(PaimonService.class)) {
            connector.connectionTest(context, item -> {});
            PaimonService probe = services.constructed().get(0);
            verify(probe, never()).startLocalRecovery(context);
            verify(probe).close();
        }
    }

    @Test
    void callbacksAndStopFromOldTaskContextCannotUseTheReplacementOwner() throws Throwable {
        PaimonConnector connector = new PaimonConnector();
        ConnectorFunctions functions = new ConnectorFunctions();
        connector.registerCapabilities(functions, new TapCodecsRegistry());
        TapConnectorContext first = recoveryContext();
        TapConnectorContext second = recoveryContext();
        when(first.getFlushOffsetCallback()).thenReturn(value -> {});
        when(second.getFlushOffsetCallback()).thenReturn(value -> {});
        HeartbeatEvent heartbeat = new HeartbeatEvent().init();
        try (MockedConstruction<PaimonConfig> ignored = recoveryConfig();
                MockedConstruction<PaimonService> services = mockConstruction(PaimonService.class)) {
            connector.onStart(first);
            PaimonService firstService = services.constructed().get(0);
            doAnswer(call -> {
                assertThrows(IllegalStateException.class, () -> connector.onStart(second));
                connector.processControl(first, heartbeat);
                return null;
            }).when(firstService).close();
            connector.onStop(first);
            verify(firstService, never()).processHeartbeat(heartbeat);

            connector.onStart(second);
            PaimonService secondService = services.constructed().get(services.constructed().size() - 1);
            connector.processControl(first, heartbeat);
            assertThrows(IllegalStateException.class, () -> functions.getWriteRecordFunction()
                    .writeRecord(first, Collections.emptyList(), new TapTable("stale"), result -> {}));
            connector.onStop(first);
            verify(secondService, never()).processHeartbeat(heartbeat);
            verify(secondService, never()).close();
            connector.processControl(second, heartbeat);
            verify(secondService).processHeartbeat(heartbeat);
            connector.onStop(second);
        }
    }

    private static TapConnectorContext recoveryContext() {
        TapConnectorContext context = mock(TapConnectorContext.class);
        when(context.getConnectionConfig()).thenReturn(DataMap.create().kv("warehouse", "/tmp/test"));
        when(context.getNodeConfig()).thenReturn(DataMap.create().kv("localRecoveryDir", "/tmp/state"));
        when(context.getLog()).thenReturn(mock(Log.class));
        return context;
    }

    private static MockedConstruction<PaimonConfig> recoveryConfig() {
        return mockConstruction(PaimonConfig.class, (config, construction) -> {
            when(config.load(anyMap())).thenReturn(config);
            when(config.isLocalRecoveryEnabled()).thenReturn(true);
        });
    }

    private static void setService(PaimonConnector connector, PaimonService service)
            throws Exception {
        Field field = PaimonConnector.class.getDeclaredField("paimonService");
        field.setAccessible(true);
        field.set(connector, service);
    }
}
