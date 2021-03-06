package com.github.mmolimar.kafka.connect.fs.task;

import com.github.mmolimar.kafka.connect.fs.FsSourceTask;
import com.github.mmolimar.kafka.connect.fs.FsSourceTaskConfig;
import com.github.mmolimar.kafka.connect.fs.file.reader.AvroFileReader;
import com.github.mmolimar.kafka.connect.fs.file.reader.TextFileReader;
import com.github.mmolimar.kafka.connect.fs.policy.Policy;
import com.github.mmolimar.kafka.connect.fs.policy.SimplePolicy;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.support.membermodification.MemberModifier;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class FsSourceTaskTest {

    private static final List<TaskFsTestConfig> TEST_FILE_SYSTEMS = Arrays.asList(
            new LocalFsConfig(),
            new HdfsFsConfig()
    );
    private static final int NUM_RECORDS = 10;

    @BeforeAll
    public static void initFs() throws IOException {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.initFs();
        }
    }

    @AfterAll
    public static void finishFs() throws IOException {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            fsConfig.close();
        }
    }

    @BeforeEach
    public void initTask() {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            Map<String, String> taskConfig = new HashMap<String, String>() {{
                String[] uris = fsConfig.getDirectories().stream().map(Path::toString)
                        .toArray(String[]::new);
                put(FsSourceTaskConfig.FS_URIS, String.join(",", uris));
                put(FsSourceTaskConfig.TOPIC, "topic_test");
                put(FsSourceTaskConfig.POLICY_CLASS, SimplePolicy.class.getName());
                put(FsSourceTaskConfig.FILE_READER_CLASS, TextFileReader.class.getName());
                put(FsSourceTaskConfig.POLICY_REGEXP, "^[0-9]*\\.txt$");
            }};

            //Mock initialization
            SourceTaskContext taskContext = PowerMock.createMock(SourceTaskContext.class);
            OffsetStorageReader offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);

            EasyMock.expect(taskContext.offsetStorageReader())
                    .andReturn(offsetStorageReader);

            EasyMock.expect(taskContext.offsetStorageReader())
                    .andReturn(offsetStorageReader);

            EasyMock.expect(offsetStorageReader.offset(EasyMock.anyObject()))
                    .andReturn(new HashMap<String, Object>() {{
                        put("offset", (long) (NUM_RECORDS / 2));
                    }});
            EasyMock.expect(offsetStorageReader.offset(EasyMock.anyObject()))
                    .andReturn(new HashMap<String, Object>() {{
                        put("offset", (long) (NUM_RECORDS / 2));
                    }});

            EasyMock.checkOrder(taskContext, false);
            EasyMock.replay(taskContext);

            EasyMock.checkOrder(offsetStorageReader, false);
            EasyMock.replay(offsetStorageReader);

            FsSourceTask task = new FsSourceTask();
            task.initialize(taskContext);

            fsConfig.setTaskConfig(taskConfig);
            fsConfig.setTask(task);
        }
    }

    @AfterEach
    public void cleanDirsAndStop() throws IOException {
        for (TaskFsTestConfig fsConfig : TEST_FILE_SYSTEMS) {
            for (Path dir : fsConfig.getDirectories()) {
                fsConfig.getFs().delete(dir, true);
                fsConfig.getFs().mkdirs(dir);
            }
            fsConfig.getTask().stop();
        }
    }

    private static Stream<Arguments> fileSystemConfigProvider() {
        return TEST_FILE_SYSTEMS.stream().map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void pollNoData(TaskFsTestConfig fsConfig) {
        fsConfig.getTask().start(fsConfig.getTaskConfig());
        assertEquals(0, fsConfig.getTask().poll().size());
        //policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void emptyFilesToProcess(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            fsConfig.getFs().createNewFile(new Path(dir, System.nanoTime() + ".txt"));
            //this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }
        fsConfig.getTask().start(fsConfig.getTaskConfig());
        assertEquals(0, fsConfig.getTask().poll().size());
        //policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void oneFilePerFs(TaskFsTestConfig fsConfig) throws IOException {
        for (Path dir : fsConfig.getDirectories()) {
            Path dataFile = new Path(dir, System.nanoTime() + ".txt");
            createDataFile(fsConfig.getFs(), dataFile);
            //this file does not match the regexp
            fsConfig.getFs().createNewFile(new Path(dir, String.valueOf(System.nanoTime())));
        }

        fsConfig.getTask().start(fsConfig.getTaskConfig());
        List<SourceRecord> records = fsConfig.getTask().poll();
        assertEquals((NUM_RECORDS * fsConfig.getDirectories().size()) / 2, records.size());
        checkRecords(records);
        //policy has ended
        assertNull(fsConfig.getTask().poll());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void nonExistentUri(TaskFsTestConfig fsConfig) {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        props.put(FsSourceTaskConfig.FS_URIS,
                new Path(fsConfig.getFs().getWorkingDirectory(), UUID.randomUUID().toString()).toString());
        fsConfig.getTask().start(props);
        fsConfig.getTask().poll();
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void exceptionExecutingPolicy(TaskFsTestConfig fsConfig) throws IOException, IllegalAccessException {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        fsConfig.getTask().start(props);

        Policy policy = EasyMock.createNiceMock(Policy.class);
        EasyMock.expect(policy.hasEnded()).andReturn(Boolean.FALSE);
        EasyMock.expect(policy.execute()).andThrow(new ConnectException("Exception from mock"));
        EasyMock.expect(policy.getURIs()).andReturn(null);
        EasyMock.checkOrder(policy, false);
        EasyMock.replay(policy);
        MemberModifier.field(FsSourceTask.class, "policy").set(fsConfig.getTask(), policy);

        assertEquals(0, fsConfig.getTask().poll().size());
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void exceptionReadingFile(TaskFsTestConfig fsConfig) throws IOException {
        Map<String, String> props = new HashMap<>(fsConfig.getTaskConfig());
        File tmp = File.createTempFile("test-", ".txt");
        try (PrintWriter writer = new PrintWriter(tmp)) {
            writer.append("txt");
        }
        Path dest = new Path(fsConfig.getDirectories().get(0).toString(), System.nanoTime() + ".txt");
        fsConfig.getFs().moveFromLocalFile(new Path(tmp.getAbsolutePath()), dest);
        props.put(FsSourceTaskConfig.FILE_READER_CLASS, AvroFileReader.class.getName());
        fsConfig.getTask().start(props);
        assertEquals(0, fsConfig.getTask().poll().size());
        fsConfig.getTask().stop();

        fsConfig.getFs().delete(dest, false);
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void nullProperties(TaskFsTestConfig fsConfig) {
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(null));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void expectedFsUris(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.remove(FsSourceTaskConfig.FS_URIS);
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void expectedPolicyClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.remove(FsSourceTaskConfig.POLICY_CLASS);
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidPolicyClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.put(FsSourceTaskConfig.POLICY_CLASS, Object.class.getName());
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void expectedReaderClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.remove(FsSourceTaskConfig.FILE_READER_CLASS);
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void invalidReaderClass(TaskFsTestConfig fsConfig) {
        Map<String, String> testProps = new HashMap<>(fsConfig.getTaskConfig());
        testProps.put(FsSourceTaskConfig.FILE_READER_CLASS, Object.class.getName());
        assertThrows(ConnectException.class, () -> fsConfig.getTask().start(testProps));
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void minimumConfig(TaskFsTestConfig fsConfig) {
        fsConfig.getTask().start(fsConfig.getTaskConfig());
        fsConfig.getTask().stop();
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void pollWithoutStart(TaskFsTestConfig fsConfig) {
        assertNull(fsConfig.getTask().poll());
        fsConfig.getTask().stop();
    }

    @ParameterizedTest
    @MethodSource("fileSystemConfigProvider")
    public void checkVersion(TaskFsTestConfig fsConfig) {
        assertNotNull(fsConfig.getTask().version());
        assertFalse("unknown".equalsIgnoreCase(fsConfig.getTask().version()));
    }

    protected void checkRecords(List<SourceRecord> records) {
        records.forEach(record -> {
            assertEquals("topic_test", record.topic());
            assertNotNull(record.sourcePartition());
            assertNotNull(record.sourceOffset());
            assertNotNull(record.value());

            assertNotNull(((Struct) record.value()).get(TextFileReader.FIELD_NAME_VALUE_DEFAULT));
        });
    }

    protected void createDataFile(FileSystem fs, Path path) throws IOException {
        File file = fillDataFile();
        fs.moveFromLocalFile(new Path(file.getAbsolutePath()), path);
    }

    private File fillDataFile() throws IOException {
        File txtFile = File.createTempFile("test-", ".txt");
        try (FileWriter writer = new FileWriter(txtFile)) {

            IntStream.range(0, NUM_RECORDS).forEach(index -> {
                String value = String.format("%d_%s", index, UUID.randomUUID());
                try {
                    writer.append(value + "\n");
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                }
            });
        }
        return txtFile;
    }

}
