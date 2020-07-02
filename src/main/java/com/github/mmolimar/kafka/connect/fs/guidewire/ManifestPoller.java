package com.github.mmolimar.kafka.connect.fs.guidewire;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ManifestPoller implements Runnable {

    private static final String MANIFEST_FILE = "manifest.json";
    private static final String LAST_SUCCESSFUL_WRITE_TIMESTAMP = "lastSuccessfulWriteTimestamp";
    private static Logger log = LoggerFactory.getLogger(ManifestPoller.class);

    private final FileSystem fs;
    private final List<String> topicNames;
    private final long pollFrequencyMs;
    private final ObjectMapper mapper;
    private final Path manifestFilePath;
    private final Manifest manifest;
    private boolean running;

    public ManifestPoller(FileSystem fs, List<String> topicNames, long pollFrequencyMs) {

        this.fs = fs;
        this.manifestFilePath = new Path(new Path(fs.getWorkingDirectory().toUri()), MANIFEST_FILE);
        this.topicNames = topicNames;
        this.pollFrequencyMs = pollFrequencyMs;
        this.mapper = new ObjectMapper();
        this.manifest = new Manifest();

        // start up poller
        new Thread(this).start();
    }

    public Manifest getManifest() {
        return this.manifest;
    }

    @Override
    public void run() {

        this.running = true;
        while(running) {
            try {
                log.debug("Updating manifest from manifest.json...");
                // update the in-memory manifest with the latest timestamp data from manifest.json
                getLatestTimestamps().forEach(this.manifest::update);
                this.manifest.setReady();
                log.debug("Completed updating manifest from manifest.json...");
            } catch (IOException e) {
                log.warn("Error reading in timestamps from manifest.json", e);
            }
            try {
                Thread.sleep(pollFrequencyMs);
            } catch (InterruptedException e) {
                log.warn("Error sleeping for manifest.json polling", e);
            }
        }
    }

    public void close() {
        this.running = false;
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