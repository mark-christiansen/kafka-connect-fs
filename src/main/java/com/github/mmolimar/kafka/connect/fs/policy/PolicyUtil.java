package com.github.mmolimar.kafka.connect.fs.policy;

import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class PolicyUtil {

    public static Map<String, Object> getPolicyConfigs(FsSourceTaskConfig conf) {
        return conf.originals().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static List<FileSystem> getFileSystems(List<String> uris, Map<String, Object> customConfigs) throws IOException {
        List<FileSystem> fileSystems = new ArrayList<>();
        for (String uri : uris) {
            fileSystems.add(getFileSystem(uri, customConfigs));
        }
        return fileSystems;
    }

    public static FileSystem getFileSystem(String uri, Map<String, Object> customConfigs) throws IOException {
        Configuration fsConfig = new Configuration();
        customConfigs.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(FsSourceTaskConfig.POLICY_PREFIX_FS))
                .forEach(entry -> fsConfig.set(entry.getKey().replace(FsSourceTaskConfig.POLICY_PREFIX_FS, ""),
                        (String) entry.getValue()));

        Path workingDir = new Path(convert(uri));
        FileSystem fs = FileSystem.newInstance(workingDir.toUri(), fsConfig);
        fs.setWorkingDirectory(workingDir);
        return fs;
    }

    private static String convert(String uri) {
        String converted = uri;
        LocalDateTime dateTime = LocalDateTime.now();
        DateTimeFormatter formatter;

        Pattern pattern = Pattern.compile("\\$\\{([a-zA-Z]+)}");
        Matcher matcher = pattern.matcher(uri);
        while (matcher.find()) {
            try {
                formatter = DateTimeFormatter.ofPattern(matcher.group(1));
                converted = converted.replaceAll("\\$\\{" + matcher.group(1) + "}", dateTime.format(formatter));
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot convert dynamic URI: " + matcher.group(1), e);
            }
        }
        return converted;
    }
}
