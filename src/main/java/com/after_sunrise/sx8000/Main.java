package com.after_sunrise.sx8000;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.opencsv.CSVWriter;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.utils.CountingOutputStream;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Objects;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * Utility to execute and export SQL results to a CSV file.
 *
 * @author takanori.takase
 * @version 0.0.0
 */
public class Main {

    public static void main(String[] args) throws Exception {

        Main main = new Main();

        JCommander.newBuilder().addObject(main).build().parse(args);

        main.execute();

    }

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());

    @Parameter(names = {"-d", "--driver"}, description = "JDBC driver class.")
    private String jdbcDriver = "org.h2.Driver";

    @Parameter(names = {"-u", "--url"}, description = "JDBC URL.")
    private String jdbcUrl = "jdbc:h2:mem:";

    @Parameter(names = {"-l", "--user"}, description = "JDBC login user.")
    private String jdbcUser = "sa";

    @Parameter(names = {"-p", "--pass"}, description = "JDBC login password.")
    private String jdbcPass = "";

    @Parameter(names = {"-s", "--statement"}, description = "JDBC SQL statement.")
    private String jdbcQuery = "select now() as \"time\"";

    @Parameter(names = {"-o", "--out"}, description = "File output path.")
    private Path out = Paths.get(System.getProperty("java.io.tmpdir"), String.format("sx8000_%s.csv", System.currentTimeMillis()));

    @Parameter(names = {"-w", "--write"}, description = "File write options.")
    private StandardOpenOption option = TRUNCATE_EXISTING;

    @Parameter(names = {"-e", "--encoding"}, description = "File encoding.")
    private String encoding = StandardCharsets.UTF_8.name();

    @Parameter(names = {"-c", "--separator"}, description = "CSV column separator character.")
    private char csvSeparator = CSVWriter.DEFAULT_SEPARATOR;

    @Parameter(names = {"-q", "--quote"}, description = "CSV column quote character.")
    private char csvQuoteChar = CSVWriter.DEFAULT_QUOTE_CHARACTER;

    @Parameter(names = {"-x", "--escape"}, description = "CSV escape character.")
    private char csvEscapeChar = CSVWriter.DEFAULT_ESCAPE_CHARACTER;

    @Parameter(names = {"-t", "--terminator"}, description = "CSV line terminator.")
    private String csvLineEnd = CSVWriter.DEFAULT_LINE_END;

    @Parameter(names = {"-h", "--header"}, description = "Include CSV header row.", arity = 1)
    private boolean csvHeader = true;

    @Parameter(names = {"-f", "--flush"}, description = "Flush per lines.")
    private int flush = 0;

    void execute() throws Exception {

        logger.info("Initializing : " + jdbcDriver);

        Class.forName(jdbcDriver);

        logger.info(String.format("Connecting : %s (user=%s)", jdbcUrl, jdbcUser));

        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPass);
             Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(readQuery(jdbcQuery))) {

            logger.info(String.format("Writing to : %s (option=%s / encoding=%s)", out, option, encoding));

            try (OutputStream os = openOutput(out, CREATE, WRITE, option);
                 CountingOutputStream co = new CountingOutputStream(os);
                 Writer writer = new OutputStreamWriter(co, encoding);
                 CSVWriter csv = new CSVWriter(writer, csvSeparator, csvQuoteChar, csvEscapeChar, csvLineEnd)) {

                ResultSetMetaData meta = rs.getMetaData();

                String[] values = new String[meta.getColumnCount()];

                if (csvHeader) {

                    for (int i = 0; i < meta.getColumnCount(); i++) {
                        values[i] = meta.getColumnLabel(i + 1);
                    }

                    csv.writeNext(values);

                }

                long count = 0;

                while (rs.next()) {

                    for (int i = 0; i < values.length; i++) {

                        Object object = rs.getObject(i + 1);

                        values[i] = Objects.toString(object, null);

                    }

                    csv.writeNext(values);

                    count++;

                    if (shouldFlush(count, flush)) {

                        csv.flush();

                        logger.info(String.format("Flushed %,3d lines... (%,3d bytes)", count, co.getBytesWritten()));

                    }

                }

                csv.flush();

                logger.info(String.format("Finished output : %,3d lines (%,3d bytes)", count, co.getBytesWritten()));

            }

        }

    }

    String readQuery(String sql) throws IOException {

        Matcher cp = Pattern.compile("^(classpath|cp):(.+)").matcher(sql);

        if (cp.matches()) {

            String path = cp.group(2);

            logger.info("Loading statement from classpath : " + path);

            byte[] bytes = new byte[4096];

            ByteArrayOutputStream out = new ByteArrayOutputStream();

            ClassLoader loader = Optional.ofNullable(
                    Thread.currentThread().getContextClassLoader()).orElseGet(Main.class::getClassLoader);

            try (InputStream in = loader.getResourceAsStream(path)) {

                for (int i = 0; i != -1; i = in.read(bytes)) {
                    out.write(bytes, 0, i);
                }

                return new String(out.toByteArray(), StandardCharsets.UTF_8);

            }

        }

        Matcher fp = Pattern.compile("^file(path)?:(.+)").matcher(sql);

        if (fp.matches()) {

            String path = fp.group(2);

            logger.info("Loading statement from filepath : " + path);

            return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);

        }

        return sql;

    }

    OutputStream openOutput(Path path, OpenOption... options) throws IOException {

        OutputStream stream = new BufferedOutputStream(Files.newOutputStream(path, options));

        String name = path.getFileName().toString();

        if (name.endsWith(".deflate")) {
            return new DeflaterOutputStream(stream);
        }

        if (name.endsWith(".gz")) {
            return new GZIPOutputStream(stream);
        }

        if (name.endsWith(".bz2")) {
            return new BZip2CompressorOutputStream(stream);
        }

        return stream;

    }

    boolean shouldFlush(long count, int flush) {

        return flush > 0 && count % flush == 0;

    }

}
