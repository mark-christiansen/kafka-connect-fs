package com.github.mmolimar.kafka.connect.fs.guidewire;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ManifestTest {

    @Test
    public void testCommitted() throws Exception {

        URI uri = Manifest.class.getResource("/guidewire").toURI();
        FileSystem fs = FileSystem.newInstance(uri, new Configuration());
        fs.setWorkingDirectory(new Path(uri));
        List<String> topicNames = new ArrayList<String>() {{
            this.add("pc_account");
        }};

        Manifest manifest = new Manifest(fs, topicNames);
        assertTrue(manifest.committed("pc_account", 1592558219702L));
        assertFalse(manifest.committed("pc_account", 1592558219703L));
        assertFalse(manifest.committed("other", 1592558219702L));
    }

    @Test
    public void testTopicNames() throws Exception {

        URI uri = Manifest.class.getResource("/guidewire").toURI();
        FileSystem fs = FileSystem.newInstance(uri, new Configuration());
        fs.setWorkingDirectory(new Path(uri));

        Manifest manifest = new Manifest(fs);
        List<String> topicNames = manifest.topicNames();
        assertEquals(2, topicNames.size());
        assertFalse(manifest.committed("pc_account", 1592558219703L));
        assertFalse(manifest.committed("other", 1592558219702L));
    }
}
