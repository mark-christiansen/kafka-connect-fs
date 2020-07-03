package com.github.mmolimar.kafka.connect.fs;

import com.github.mmolimar.kafka.connect.fs.guidewire.Manifest;
import com.github.mmolimar.kafka.connect.fs.policy.PolicyUtil;
import com.github.mmolimar.kafka.connect.fs.util.Version;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FsSourceConnector extends SourceConnector {

    private static Logger log = LoggerFactory.getLogger(FsSourceConnector.class);

    private FsSourceConnectorConfig config;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting FsSourceConnector...");
        try {
            config = new FsSourceConnectorConfig(properties);
        } catch (ConfigException ce) {
            throw new ConnectException("Couldn't start FsSourceConnector due to configuration error.", ce);
        } catch (Exception ce) {
            throw new ConnectException("An error has occurred when starting FsSourceConnector." + ce);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FsSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        if (config == null) {
            throw new ConnectException("Connector config has not been initialized.");
        }
        final List<Map<String, String>> taskConfigs = new ArrayList<>();

        // see if the Guidewire manifest polling is enabled
        FsSourceTaskConfig taskConfig = new FsSourceTaskConfig(config.originalsStrings());
        boolean manifestPollEnabled = taskConfig.getBoolean(FsSourceTaskConfig.MANIFEST_POLL_ENABLED);
        if (manifestPollEnabled) {

            // assume one FS URI and get the topics from the Guidewire manifest.json file
            String fsUri = config.getFsUris().get(0);
            List<String> topicNames = new ArrayList<>();
            try {
                FileSystem fs = PolicyUtil.getFileSystem(fsUri, PolicyUtil.getPolicyConfigs(taskConfig));
                Manifest manifest = new Manifest(fs);
                topicNames = manifest.topicNames();
            } catch (IOException e) {
                log.error("Error retrieving Guidewire manifest for verifying file commit status", e);
            }

            // create one task per topic
            List<String> fsUris = topicNames.stream().map(topicName -> fsUri + topicName).collect(Collectors.toList());
            for (int i = 0; i < fsUris.size(); i++) {
                Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
                taskProps.put(FsSourceConnectorConfig.FS_URIS, fsUris.get(i));
                taskProps.put(FsSourceConnectorConfig.TOPIC, topicNames.get(i));
                taskConfigs.add(taskProps);
            }

        } else {

            List<String> fsUris = config.getFsUris();
            int groups = Math.min(fsUris.size(), maxTasks);
            ConnectorUtils.groupPartitions(fsUris, groups)
                    .forEach(dirs -> {
                        Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
                        taskProps.put(FsSourceConnectorConfig.FS_URIS, String.join(",", dirs));
                        taskConfigs.add(taskProps);
                    });
            log.debug("Partitions grouped as: {}", taskConfigs);
        }

        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping FsSourceConnector.");
        //Nothing to do
    }

    @Override
    public ConfigDef config() {
        return FsSourceConnectorConfig.conf();
    }
}
