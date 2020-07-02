package com.github.mmolimar.kafka.connect.fs;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class FsSourceTaskConfig extends FsSourceConnectorConfig {

    public static final String POLICY_PREFIX = "policy.";
    public static final String FILE_READER_PREFIX = "file_reader.";

    public static final String POLICY_CLASS = POLICY_PREFIX + "class";
    private static final String POLICY_CLASS_DOC = "Policy class to apply to this task.";
    private static final String POLICY_CLASS_DISPLAY = "Policy";

    public static final String POLICY_RECURSIVE = POLICY_PREFIX + "recursive";
    private static final String POLICY_RECURSIVE_DOC = "Flag to activate traversed recursion in subdirectories when listing files.";
    private static final String POLICY_RECURSIVE_DISPLAY = "Recursive directory listing";

    public static final String POLICY_REGEXP = POLICY_PREFIX + "regexp";
    private static final String POLICY_REGEXP_DOC = "Regular expression to filter files from the FS.";
    private static final String POLICY_REGEXP_DISPLAY = "File filter regex";

    public static final String POLICY_PREFIX_FS = POLICY_PREFIX + "fs.";

    public static final String FILE_READER_CLASS = FILE_READER_PREFIX + "class";
    private static final String FILE_READER_CLASS_DOC = "File reader class to read files from the FS.";
    private static final String FILE_READER_CLASS_DISPLAY = "File reader class";

    public static final String POLL_INTERVAL_MS = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 10000;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String MANIFEST_POLL_ENABLED = "manifest.poll.enabled";
    private static final String MANIFEST_POLL_ENABLED_DOC = "Polling enabled for reading Guidewire manifest data.";
    public static final boolean MANIFEST_POLL_ENABLED_DEFAULT = false;
    private static final String MANIFEST_POLL_ENABLED_DISPLAY = "Guidewire manifest polling enabled";

    public static final String MANIFEST_POLL_INTERVAL_MS = "manifest.poll.interval.ms";
    private static final String MANIFEST_POLL_INTERVAL_MS_DOC = "Poll interval for reading Guidewire manifest data.";
    public static final int MANIFEST_POLL_INTERVAL_MS_DEFAULT = 30000;
    private static final String MANIFEST_POLL_INTERVAL_MS_DISPLAY = "Guidewire manifest polling interval (ms)";

    private static final String POLICY_GROUP = "Policy";
    private static final String CONNECTOR_GROUP = "Connector";

    public FsSourceTaskConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public FsSourceTaskConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        int order = 0;
        return FsSourceConnectorConfig.conf()
                .define(
                        POLICY_CLASS,
                        ConfigDef.Type.CLASS,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        POLICY_CLASS_DOC,
                        POLICY_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        POLICY_CLASS_DISPLAY
                ).define(
                        POLICY_RECURSIVE,
                        ConfigDef.Type.BOOLEAN,
                        Boolean.TRUE,
                        ConfigDef.Importance.MEDIUM,
                        POLICY_RECURSIVE_DOC,
                        POLICY_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        POLICY_RECURSIVE_DISPLAY
                ).define(
                        POLICY_REGEXP,
                        ConfigDef.Type.STRING,
                        ".*",
                        ConfigDef.Importance.MEDIUM,
                        POLICY_REGEXP_DOC,
                        POLICY_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        POLICY_REGEXP_DISPLAY
                ).define(
                        FILE_READER_CLASS,
                        ConfigDef.Type.CLASS,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        FILE_READER_CLASS_DOC,
                        POLICY_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        FILE_READER_CLASS_DISPLAY
                ).define(
                        POLL_INTERVAL_MS,
                        ConfigDef.Type.INT,
                        POLL_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        POLL_INTERVAL_MS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.SHORT,
                        POLL_INTERVAL_MS_DISPLAY
                ).define(
                        MANIFEST_POLL_ENABLED,
                        ConfigDef.Type.BOOLEAN,
                        MANIFEST_POLL_ENABLED_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        MANIFEST_POLL_ENABLED_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        MANIFEST_POLL_ENABLED_DISPLAY
                ).define(
                        MANIFEST_POLL_INTERVAL_MS,
                        ConfigDef.Type.INT,
                        MANIFEST_POLL_INTERVAL_MS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        MANIFEST_POLL_INTERVAL_MS_DOC,
                        CONNECTOR_GROUP,
                        ++order,
                        ConfigDef.Width.MEDIUM,
                        MANIFEST_POLL_INTERVAL_MS_DISPLAY
                );
    }
}
