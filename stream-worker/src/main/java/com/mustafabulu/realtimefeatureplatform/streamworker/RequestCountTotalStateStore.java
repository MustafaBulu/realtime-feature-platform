package com.mustafabulu.realtimefeatureplatform.streamworker;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class RequestCountTotalStateStore implements AutoCloseable {

    private final RocksDB database;

    RequestCountTotalStateStore(@Value("${rfp.rocksdb.path}") Path databasePath) {
        RocksDB.loadLibrary();
        this.database = open(databasePath);
    }

    boolean markIfAbsent(String key, Instant expiresAt, Instant now) {
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        try {
            byte[] existing = database.get(encodedKey);
            if (existing != null && !isExpired(existing, now)) {
                return false;
            }
            database.put(encodedKey, Long.toString(expiresAt.toEpochMilli()).getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not update RocksDB marker", ex);
        }
    }

    boolean containsUnexpiredMarker(String key, Instant now) {
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        try {
            byte[] existing = database.get(encodedKey);
            return existing != null && !isExpired(existing, now);
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not read RocksDB marker", ex);
        }
    }

    void cleanupExpiredMarkers(String prefix, Instant now, int maxEntries) {
        if (maxEntries <= 0) {
            return;
        }

        byte[] encodedPrefix = prefix.getBytes(StandardCharsets.UTF_8);
        List<byte[]> expiredKeys = new ArrayList<>();
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seek(encodedPrefix);
                    iterator.isValid()
                            && startsWith(iterator.key(), encodedPrefix)
                            && expiredKeys.size() < maxEntries;
                    iterator.next()) {
                if (isExpired(iterator.value(), now)) {
                    expiredKeys.add(iterator.key().clone());
                }
            }
        }

        try {
            for (byte[] expiredKey : expiredKeys) {
                database.delete(expiredKey);
            }
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not cleanup expired RocksDB markers", ex);
        }
    }

    String get(String key) {
        try {
            byte[] value = database.get(key.getBytes(StandardCharsets.UTF_8));
            return value == null ? null : new String(value, StandardCharsets.UTF_8);
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not read RocksDB state", ex);
        }
    }

    void put(String key, String value) {
        try {
            database.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not write RocksDB state", ex);
        }
    }

    List<StateEntry> entriesWithPrefix(String prefix) {
        byte[] encodedPrefix = prefix.getBytes(StandardCharsets.UTF_8);
        Map<String, String> entries = new TreeMap<>(Comparator.naturalOrder());
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seek(encodedPrefix);
                    iterator.isValid() && startsWith(iterator.key(), encodedPrefix);
                    iterator.next()) {
                entries.put(
                        new String(iterator.key(), StandardCharsets.UTF_8),
                        new String(iterator.value(), StandardCharsets.UTF_8)
                );
            }
        }
        return entries.entrySet().stream()
                .map(entry -> new StateEntry(entry.getKey(), entry.getValue()))
                .toList();
    }

    void flush() {
        try (FlushOptions options = new FlushOptions().setWaitForFlush(true)) {
            database.flush(options);
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not flush RocksDB state", ex);
        }
    }

    long estimatedStateSizeBytes() {
        long size = 0L;
        try (RocksIterator iterator = database.newIterator()) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                size += iterator.key().length;
                size += iterator.value().length;
            }
        }
        return size;
    }

    @PreDestroy
    @Override
    public void close() {
        database.close();
    }

    private static RocksDB open(Path databasePath) {
        try (Options options = new Options().setCreateIfMissing(true)) {
            return open(databasePath, options);
        }
    }

    private static RocksDB open(Path databasePath, Options options) {
        try {
            Files.createDirectories(databasePath);
            return RocksDB.open(options, databasePath.toString());
        } catch (Exception ex) {
            throw new IllegalStateException("Could not open RocksDB at " + databasePath, ex);
        }
    }

    private static boolean isExpired(byte[] value, Instant now) {
        try {
            return Long.parseLong(new String(value, StandardCharsets.UTF_8)) <= now.toEpochMilli();
        } catch (NumberFormatException ex) {
            return true;
        }
    }

    private static boolean startsWith(byte[] value, byte[] prefix) {
        if (value.length < prefix.length) {
            return false;
        }
        for (int index = 0; index < prefix.length; index++) {
            if (value[index] != prefix[index]) {
                return false;
            }
        }
        return true;
    }

    record StateEntry(String key, String value) {
    }
}
