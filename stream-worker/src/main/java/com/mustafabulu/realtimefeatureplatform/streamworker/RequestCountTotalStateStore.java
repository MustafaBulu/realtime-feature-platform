package com.mustafabulu.realtimefeatureplatform.streamworker;

import jakarta.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class RequestCountTotalStateStore {

    private final RocksDB database;
    private final Options options;

    RequestCountTotalStateStore(@Value("${rfp.rocksdb.path}") Path databasePath) {
        RocksDB.loadLibrary();
        this.options = new Options().setCreateIfMissing(true);
        this.database = open(databasePath, options);
    }

    long add(String key, long increment) {
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        long previous = readLong(encodedKey);
        long next = previous + increment;
        writeLong(encodedKey, next);
        return next;
    }

    boolean markIfAbsent(String key) {
        byte[] encodedKey = key.getBytes(StandardCharsets.UTF_8);
        try {
            if (database.get(encodedKey) != null) {
                return false;
            }
            database.put(encodedKey, "1".getBytes(StandardCharsets.UTF_8));
            return true;
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not update RocksDB marker", ex);
        }
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

    int cleanupExpiredMarkers(String prefix, Instant now, int maxEntries) {
        if (maxEntries <= 0) {
            return 0;
        }

        byte[] encodedPrefix = prefix.getBytes(StandardCharsets.UTF_8);
        List<byte[]> expiredKeys = new ArrayList<>();
        RocksIterator iterator = database.newIterator();
        try {
            for (iterator.seek(encodedPrefix);
                    iterator.isValid()
                            && startsWith(iterator.key(), encodedPrefix)
                            && expiredKeys.size() < maxEntries;
                    iterator.next()) {
                if (isExpired(iterator.value(), now)) {
                    expiredKeys.add(iterator.key().clone());
                }
            }
        } finally {
            iterator.close();
        }

        try {
            for (byte[] expiredKey : expiredKeys) {
                database.delete(expiredKey);
            }
            return expiredKeys.size();
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

    @PreDestroy
    void close() {
        database.close();
        options.close();
    }

    private static RocksDB open(Path databasePath, Options options) {
        try {
            Files.createDirectories(databasePath);
            return RocksDB.open(options, databasePath.toString());
        } catch (Exception ex) {
            throw new IllegalStateException("Could not open RocksDB at " + databasePath, ex);
        }
    }

    private long readLong(byte[] key) {
        try {
            byte[] value = database.get(key);
            return value == null ? 0L : Long.parseLong(new String(value, StandardCharsets.UTF_8));
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not read RocksDB state", ex);
        }
    }

    private void writeLong(byte[] key, long value) {
        try {
            database.put(key, Long.toString(value).getBytes(StandardCharsets.UTF_8));
        } catch (RocksDBException ex) {
            throw new IllegalStateException("Could not write RocksDB state", ex);
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
}
