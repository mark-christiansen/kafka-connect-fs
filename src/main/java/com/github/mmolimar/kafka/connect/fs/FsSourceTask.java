package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.file.FileMetadata;
import com.github.mmolimar.kafka.connect.fs.file.reader.FileReader;
import com.github.mmolimar.kafka.connect.fs.guidewire.Manifest;
import com.github.mmolimar.kafka.connect.fs.guidewire.ManifestPoller;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.policy.PolicyUtil;
import com.github.mmolimar.kafka.connect.fs.util.ReflectionUtils;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FsSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(FsSourceTask.class);

    private final AtomicBoolean stop;
    private final Time time;

    private FsSourceTaskConfig config;
    private Policy policy;
    private int pollInterval;

    private ManifestPoller manifestPoller;

    public FsSourceTask() {
        this.stop = new AtomicBoolean(false);
        this.time = new SystemTime();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting FS source task...");
        try {
            config = new FsSourceTaskConfig(properties);
            if (config.getClass(FsSourceTaskConfig.POLICY_CLASS).isAssignableFrom(Policy.class)) {
                throw new ConfigException("Policy class " +
                        config.getClass(FsSourceTaskConfig.POLICY_CLASS) + " is not a subclass of " + Policy.class);
            }
            if (config.getClass(FsSourceTaskConfig.FILE_READER_CLASS).isAssignableFrom(FileReader.class)) {
                throw new ConfigException("FileReader class " +
                        config.getClass(FsSourceTaskConfig.FILE_READER_CLASS) + " is not a subclass of " + FileReader.class);
            }

            Class<Policy> policyClass = (Class<Policy>) Class.forName(properties.get(FsSourceTaskConfig.POLICY_CLASS));
            policy = ReflectionUtils.makePolicy(policyClass, config);
            pollInterval = config.getInt(FsSourceTaskConfig.POLL_INTERVAL_MS);
        } catch (ConfigException ce) {
            log.error("Couldn't start FsSourceTask.", ce);
            throw new ConnectException("Couldn't start FsSourceTask due to configuration error: " + ce.getMessage(), ce);
        } catch (Exception e) {
            log.error("Couldn't start FsSourceConnector.", e);
            throw new ConnectException("A problem has occurred reading configuration: " + e.getMessage(), e);
        }
        log.info("FS source task started with policy [{}].", policy.getClass().getName());

        // Setup and start manifest poller for Guidewire
        boolean enabled = config.getBoolean(FsSourceTaskConfig.MANIFEST_POLL_ENABLED);
        log.info("Guidewire Manifest poller enabled {}", enabled);
        if (enabled) {

            log.info("Starting Guidewire Manifest poller...");
            try {

                String uri = config.getFsUris().get(0);
                if (uri.endsWith("/")) {
                    uri = uri.substring(0, uri.length() - 1);
                }
                uri = uri.substring(0, uri.lastIndexOf("/"));
                FileSystem fs = PolicyUtil.getFileSystem(uri, PolicyUtil.getPolicyConfigs(config));

                List<String> topicNames = new ArrayList<>();
                topicNames.add(config.getTopic());
                int manifestPollInterval = config.getInt(FsSourceTaskConfig.MANIFEST_POLL_INTERVAL_MS);
                this.manifestPoller = new ManifestPoller(fs, topicNames, manifestPollInterval);

            } catch (IOException e) {
                log.error("Error creating manifest poller", e);
            }
        }
    }

    @Override
    public List<SourceRecord> poll() {

        while (!stop.get() && policy != null && !policy.hasEnded()) {

            log.trace("Polling for new data...");

            // get Guidewire manifest to check that timestamp folders have been committed before returning
            // them in the poll() method
            final Manifest manifest = this.manifestPoller != null ? this.manifestPoller.getManifest() : null;
            if (manifest != null) {
                while (!manifest.isReady(10000)) {
                    log.debug("Waiting for Guidewire Manifest to be loaded");
                }
            }

            List<SourceRecord> totalRecords = filesToProcess().map(metadata -> {

                log.debug("Checking file against Guidewire manifest{}.", metadata.getPath());
                String path = metadata.getPath();
                path = path.substring(0, path.lastIndexOf("/"));
                String timestampFolder = path.substring(path.lastIndexOf("/"));

                List<SourceRecord> records = new ArrayList<>();
                log.debug("Verifying topic {} and timestamp {} are committed according to Guidewire manifest.", this.config.getTopic(), timestampFolder);
                if (manifest == null || manifest.committed(this.config.getTopic(), Long.parseLong(timestampFolder))) {

                    try (FileReader reader = policy.offer(metadata, context.offsetStorageReader())) {
                        log.info("Processing records for file {}.", metadata);
                        while (reader.hasNext()) {
                            records.add(convert(metadata, reader.currentOffset() + 1, reader.next()));
                        }
                    } catch (ConnectException | IOException e) {
                        //when an exception happens reading a file, the connector continues
                        log.error("Error reading file [{}]. Keep going...", metadata.getPath(), e);
                    }
                    log.debug("Read [{}] records from file [{}].", records.size(), metadata.getPath());

                } else {
                    log.debug("File not committed according to Guidewire manifest{}.", metadata.getPath());
                }
                return records;

            }).flatMap(Collection::stream).collect(Collectors.toList());

            log.debug("Returning [{}] records in execution number [{}] for policy [{}].",
                    totalRecords.size(), policy.getExecutions(), policy.getClass().getName());

            return totalRecords;
        }
        if (pollInterval > 0) {
            log.trace("Waiting [{}] ms for next poll.", pollInterval);
            time.sleep(pollInterval);
        }
        return null;
    }

    private Stream<FileMetadata> filesToProcess() {
        try {
            return asStream(policy.execute())
                    .filter(metadata -> metadata.getLen() > 0);
        } catch (IOException | ConnectException e) {
            //when an exception happens executing the policy, the connector continues
            log.error("Cannot retrieve files to process from the FS: {}. " +
                            "There was an error executing the policy but the task tolerates this and continues.",
                    policy.getURIs(), e);
            return Stream.empty();
        }
    }

    private <T> Stream<T> asStream(Iterator<T> src) {
        Iterable<T> iterable = () -> src;
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    private SourceRecord convert(FileMetadata metadata, long offset, Struct struct) {
        return new SourceRecord(
                Collections.singletonMap("path", metadata.getPath()),
                Collections.singletonMap("offset", offset),
                config.getTopic(),
                struct.schema(),
                struct
        );
    }

    @Override
    public void stop() {

        log.info("Stopping FS source task...");
        stop.set(true);
        synchronized (this) {
            if (policy != null) {
                try {
                    policy.close();
                } catch (IOException ioe) {
                    log.warn("Error closing policy [{}].", policy.getClass().getName(), ioe);
                }
            }
        }

        log.info("Stopping Guidewire Manifest poller...");
        if (this.manifestPoller != null) {
            this.manifestPoller.close();
        }
    }
}
