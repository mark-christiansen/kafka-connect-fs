package com.github.mmolimar.kafka.connect.fs.guidewire;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class Manifest {

    private static Logger log = LoggerFactory.getLogger(ManifestPoller.class);

    private final ConcurrentHashMap<String, Long> lastWrittenTimestamps = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private boolean ready;

    public void update(String topicName, long lastWrittenTimestamp) {
        lastWrittenTimestamps.put(topicName, lastWrittenTimestamp);
    }

    public boolean committed(String topicName, long timestamp) {
        Long lastWrittenTimestamp = lastWrittenTimestamps.get(topicName);
        return lastWrittenTimestamp != null && timestamp <= lastWrittenTimestamp;
    }

    public boolean isReady(long timeout) {
        synchronized (lock) {
            if (!ready) {
                try {
                    lock.wait(timeout);
                } catch (InterruptedException e) {
                    log.warn("Error encountered waiting for Manifest to be ready", e);
                }
            }
            return this.ready;
        }
    }

    public void setReady() {
        synchronized (lock) {
            this.ready = true;
            lock.notifyAll();
        }
    }
}