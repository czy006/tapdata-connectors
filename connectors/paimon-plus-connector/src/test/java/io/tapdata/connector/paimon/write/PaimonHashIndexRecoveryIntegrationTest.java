package io.tapdata.connector.paimon.write;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PaimonHashIndexRecoveryIntegrationTest {
    @TempDir Path root;

    @Test
    void crashedSpillingWriterRecoversItsCumulativeIndexesAfterSpillDirectoryRemoval() throws Exception {
        try (var fixture = new DynamicRecoveryFixture(root, false, Map.of())) {
            assertEquals(List.of("3@1=30", "4@3=44", "5@4=55", "6@6=60", "7@7=70"),
                    fixture.run(DynamicRecoveryFixture.Scenario.ORDERED));
        }
    }

    @Test
    void reversedNativeMessagesPassFirstReadThenCreateDuplicatePrimaryKey() throws Exception {
        try (var fixture = new DynamicRecoveryFixture(root, false, Map.of())) {
            assertEquals(List.of("3@1=30", "4@1=40", "4@3=44", "5@4=55", "6@6=60", "7@7=70"),
                    fixture.run(DynamicRecoveryFixture.Scenario.REVERSED_HASH_INDEX));
        }
    }
}
