package com.after_sunrise.sx8000;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
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
        Path hash = Paths.get(TEMPDIR, MainTest.class.getSimpleName() + ".csv.sha256");

        try {

            Main.main(null);

            List<String> options = new ArrayList<>();
            options.add("--out");
            options.add(path.toAbsolutePath().toString());
            options.add("--statement");
            options.add("select 'foo bar' as TEST;");

            Main.main(options.toArray(new String[0]));
            String content = new String(Files.readAllBytes(path), UTF_8);
            assertEquals("\"TEST\"\n\"foo bar\"\n", content);
            assertEquals(DigestUtils.sha256Hex(content), new String(Files.readAllBytes(hash), UTF_8));

            options.add("--header");
            options.add("false");
            options.add("--flush");
            options.add("1");
            Main.main(options.toArray(new String[0]));
            content = new String(Files.readAllBytes(path), UTF_8);
            assertEquals("\"foo bar\"\n", content);
            assertEquals(DigestUtils.sha256Hex(content), new String(Files.readAllBytes(hash), UTF_8));

        } finally {

            Files.deleteIfExists(path);
            Files.deleteIfExists(hash);

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
    void wrapOutput() throws IOException {

        Map<String, Class<? extends OutputStream>> outputs = new LinkedHashMap<>();
        outputs.put(".txt", ByteArrayOutputStream.class);
        outputs.put(".deflate", DeflaterOutputStream.class);
        outputs.put(".gz", GZIPOutputStream.class);
        outputs.put(".bz2", BZip2CompressorOutputStream.class);

        Main main = new Main();

        for (Map.Entry<String, Class<? extends OutputStream>> entry : outputs.entrySet()) {

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            try (OutputStream wrapped = main.wrapOutput(Paths.get("test" + entry.getKey()), out)) {

                assertEquals(entry.getValue(), wrapped.getClass(), "Suffix : " + entry.getKey());

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
