package com.after_sunrise.sx8000;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.junit.jupiter.api.Test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author takanori.takase
 * @version 0.0.0
 */
class MainTest {

    private static final String TEMPDIR = System.getProperty("java.io.tmpdir");

    @Test
    void main() throws Exception {

        Path path = Paths.get(TEMPDIR, MainTest.class.getSimpleName() + ".csv");

        try {

            List<String> options = new ArrayList<>();
            options.add("--out");
            options.add(path.toAbsolutePath().toString());
            options.add("--statement");
            options.add("select 'foo bar' as TEST;");

            Main.main(options.toArray(new String[0]));
            assertEquals("\"TEST\"\n\"foo bar\"\n", new String(Files.readAllBytes(path), UTF_8));

            options.add("--header");
            options.add("false");
            options.add("--flush");
            options.add("1");
            Main.main(options.toArray(new String[0]));
            assertEquals("\"foo bar\"\n", new String(Files.readAllBytes(path), UTF_8));

        } finally {

            Files.deleteIfExists(path);

        }

    }

    @Test
    void readQuery() throws IOException {

        List<String> paths = new ArrayList<>();
        paths.add("cp:test.sql");
        paths.add("classpath:test.sql");
        paths.add("file:src/test/resources/test.sql");
        paths.add("filepath:src/test/resources/test.sql");

        Main main = new Main();

        for (String path : paths) {
            assertEquals("select now();\n", main.readQuery(path), "Path : " + path);
        }

        assertEquals("foo bar", main.readQuery("foo bar"));

    }

    @Test
    void openOutput() throws IOException {

        Map<String, Class<? extends OutputStream>> outputs = new LinkedHashMap<>();
        outputs.put(".txt", BufferedOutputStream.class);
        outputs.put(".deflate", DeflaterOutputStream.class);
        outputs.put(".gz", GZIPOutputStream.class);
        outputs.put(".bz2", BZip2CompressorOutputStream.class);

        Main main = new Main();

        for (Map.Entry<String, Class<? extends OutputStream>> entry : outputs.entrySet()) {

            Path path = Paths.get(TEMPDIR, MainTest.class.getSimpleName() + entry.getKey());

            try {

                try (OutputStream out = main.openOutput(path, CREATE, WRITE)) {
                    assertEquals(entry.getValue(), out.getClass(), "Suffix : " + entry.getKey());
                }

            } finally {
                Files.deleteIfExists(path);
            }

        }

    }

    @Test
    void shouldFlush() {

        Main main = new Main();

        assertFalse(main.shouldFlush(0, 0));
        assertFalse(main.shouldFlush(4, 0));
        assertFalse(main.shouldFlush(7, 0));

        assertTrue(main.shouldFlush(0, 1));
        assertTrue(main.shouldFlush(4, 1));
        assertTrue(main.shouldFlush(7, 1));

        assertTrue(main.shouldFlush(0, 2));
        assertTrue(main.shouldFlush(4, 2));
        assertFalse(main.shouldFlush(7, 2));

    }

}
