package io.tapdata.connector.paimon.service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/** Exercises the registered PDK TIME codec, strong-mode writes, and the ordinary Paimon batch reader. */
public final class TemporalPrimaryKeyProbe {
    public static void main(String[] args) throws Throwable {
        Path base = Path.of(args[0]);
        String bucket = args[1];
        if (!List.of("fixed", "dynamic").contains(bucket)) {
            throw new IllegalArgumentException("Choose fixed or dynamic");
        }
        for (String operation : List.of("insert", "update", "delete")) {
            PaimonRecoveryTimeKeyIntegrationTest.verify(Files.createDirectories(base.resolve(operation)), bucket, operation);
            System.out.println("PASS " + bucket + " " + operation + ": rejected lossy key; snapshot and rows intact");
        }
    }
}
