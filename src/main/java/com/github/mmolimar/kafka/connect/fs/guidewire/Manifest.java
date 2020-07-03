package com.github.mmolimar.kafka.connect.fs.guidewire;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Manifest {

    private static final Logger log = LoggerFactory.getLogger(Manifest.class);
    private static final String MANIFEST_FILE = "manifest.json";
    private static final String LAST_SUCCESSFUL_WRITE_TIMESTAMP = "lastSuccessfulWriteTimestamp";

    private final Map<String, Long> lastWrittenTimestamps;
    private final FileSystem fs;
    private final ObjectMapper mapper;
    private final Path manifestFilePath;
    private List<String> topicNames = new ArrayList<>();

    public Manifest(FileSystem fs, List<String> topicNames) throws IOException {
        this.fs = fs;
        this.manifestFilePath = new Path(new Path(fs.getWorkingDirectory().toUri()), MANIFEST_FILE);
        this.topicNames = topicNames;
        this.mapper = new ObjectMapper();
        this.lastWrittenTimestamps = getLatestTimestamps();
    }

    public Manifest(FileSystem fs) throws IOException {
        this.fs = fs;
        this.manifestFilePath = new Path(new Path(fs.getWorkingDirectory().toUri()), MANIFEST_FILE);
        this.mapper = new ObjectMapper();
        this.lastWrittenTimestamps = getLatestTimestamps();
        lastWrittenTimestamps.forEach((key, value) -> log.debug("Guidewire Manifest: loading topic {} with last written timestamp {}", key, value));
    }

    public List<String> topicNames() throws IOException {
        // open manifest.json file and grab all the topic names out of the file
        List<String> topicNames = new ArrayList<>();
        try (BufferedInputStream in = new BufferedInputStream(this.fs.open(this.manifestFilePath))) {
            JsonNode node = this.mapper.readTree(in);
            Iterator<String> fieldNames = node.fieldNames();
            while(fieldNames.hasNext()) {
                topicNames.add(fieldNames.next());
            }
        }
        return topicNames;
    }

    public boolean committed(String topicName, long timestamp) {
        Long lastWrittenTimestamp = lastWrittenTimestamps.get(topicName);
        log.debug("Guidewire Manifest: comparing timestamp {} to last written {} for topic {}", timestamp, (lastWrittenTimestamp != null ? lastWrittenTimestamp : 0), topicName);
        return lastWrittenTimestamp != null && timestamp <= lastWrittenTimestamp;
    }

    private Map<String, Long> getLatestTimestamps() throws IOException {
        // open manifest.json file and grab the last successful write timestamp for every topic
        // in "topicNames"
        try (BufferedInputStream in = new BufferedInputStream(this.fs.open(this.manifestFilePath))) {
            JsonNode node = this.mapper.readTree(in);
            return this.topicNames.stream().collect(Collectors.toMap(
                    topicName -> topicName,
                    topicName -> Long.parseLong(node.get(topicName).get(LAST_SUCCESSFUL_WRITE_TIMESTAMP).asText())));
        }
    }
}