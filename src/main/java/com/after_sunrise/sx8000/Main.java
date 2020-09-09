package com.after_sunrise.sx8000;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.opencsv.CSVWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.utils.CountingOutputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

import static java.nio.file.StandardOpenOption.*;

/**
 * Utility to execute and export SQL results to a CSV file.
 *
 * @author takanori.takase
 * @version 0.0.0
 */
public class Main {

    public static void main(String[] args) throws Exception {

        Main main = new Main();

        JCommander commander = JCommander.newBuilder().addObject(main).build();

        if (ArrayUtils.isEmpty(args)) {

            commander.usage();

        } else {

            commander.parse(args);

            main.execute();

        }

    }

    private final Logger logger = Logger.getLogger(getClass().getSimpleName());

    @Parameter(names = {"-j", "--driver"}, description = "JDBC driver class.")
    private String jdbcDriver = "org.h2.Driver";

    @Parameter(names = {"-u", "--url"}, description = "JDBC URL.")
    private String jdbcUrl = "jdbc:h2:mem:";

    @Parameter(names = {"-l", "--user"}, description = "JDBC login user.")
    private String jdbcUser = "sa";

    @Parameter(names = {"-p", "--pass"}, description = "JDBC login password. \"classpath:\" or \"filepath:\" prefix can be used to read from a file.")
    private String jdbcPass = null;

    @Parameter(names = {"-s", "--statement"}, description = "JDBC SQL statement. \"classpath:\" or \"filepath:\" prefix can be used to read from a file.")
    private String jdbcQuery = "select now() as \"time\"";

    @Parameter(names = {"-o", "--out"}, description = "File output path.")
    private Path out = Paths.get(System.getProperty("java.io.tmpdir"), String.format("sx8000_%s.csv", System.currentTimeMillis()));

    @Parameter(names = {"-w", "--write"}, description = "File write mode. Specify \"CREATE_NEW\" to fail if the output already exists.")
    private StandardOpenOption writeMode = TRUNCATE_EXISTING;

    @Parameter(names = {"-e", "--encoding"}, description = "Output file encoding. (cf: \"ISO-8859-1\", \"UTF-16\")")
    private String encoding = StandardCharsets.UTF_8.name();

		@Parameter(names = {"-n", "--nullReplacement"}, description = "A replacement value for NULL one.")
		private String nullReplacement = null;

		@Parameter(names = {"--timestampFormatPattern"}, description = "An optional pattern to format Timestamp according to https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html#patterns.")
		private String timestampFormatPattern = null;

		@Parameter(names = {"-b", "--booleanAsInt"}, description = "Should boolean be converted to int (0, 1) ?", arity = 1)
		private boolean booleanAsInt = true;

  	@Parameter(names = {"--quoteAll"}, description = "Should all CSV column be quoted with the quote character ?", arity = 1)
  	private boolean csvQuoteAll = true;

		@Parameter(names = {"-d", "--delimiter"}, description = "CSV column delimiter character.", converter = CharacterConverter.class)
    private char csvSeparator = CSVWriter.DEFAULT_SEPARATOR;

    @Parameter(names = {"-q", "--quote"}, description = "CSV column quote character.", converter = CharacterConverter.class)
    private char csvQuoteChar = CSVWriter.DEFAULT_QUOTE_CHARACTER;

    @Parameter(names = {"-x", "--escape"}, description = "CSV escape character.", converter = CharacterConverter.class)
    private char csvEscapeChar = CSVWriter.DEFAULT_ESCAPE_CHARACTER;

    @Parameter(names = {"-t", "--terminator"}, description = "CSV line terminator.", hidden = true)
    private String csvLineEnd = CSVWriter.DEFAULT_LINE_END;

    @Parameter(names = {"-h", "--header"}, description = "Include CSV header row.", arity = 1)
    private boolean csvHeader = true;

    @Parameter(names = {"-f", "--flush"}, description = "Number of lines written to trigger intermediary flush.")
    private int flush = 0;

    @Parameter(names = {"-c", "--checksum"}, description = "Generate checksum file.", arity = 1)
    private boolean checksum = true;

    @Parameter(names = {"-a", "--algorithm"}, description = "Algorithm of the checksum. (cf: \"MD5\", \"SHA-1\")")
    private String algorithm = "SHA-256";

    void execute() throws Exception {

        logger.info("Executing...");

        Class.forName(jdbcDriver);

        MessageDigest digest = MessageDigest.getInstance(algorithm);

        logger.info(String.format("Connecting : %s (user=%s)", jdbcUrl, jdbcUser));

        try (Connection conn = DriverManager.getConnection(jdbcUrl, jdbcUser, StringUtils.chomp(readText(jdbcPass)));
             Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(readText(jdbcQuery))) {

            logger.info(String.format("Writing to : %s (mode=%s / encoding=%s)", out, writeMode, encoding));

            try (OutputStream os = Files.newOutputStream(out, CREATE, WRITE, writeMode);
                 DigestOutputStream ds = new DigestOutputStream(os, digest);
                 CountingOutputStream co = new CountingOutputStream(ds);
                 Writer writer = new BufferedWriter(new OutputStreamWriter(wrapOutput(out, co), encoding));
                 CSVWriter csv = new CSVWriter(writer, csvSeparator, csvQuoteChar, csvEscapeChar, csvLineEnd)) {

                ds.on(checksum);

                ResultSetMetaData meta = rs.getMetaData();

	              DateTimeFormatter formatter;
	              if (timestampFormatPattern==null) {
	              	formatter = null;
	              } else {
		              formatter = DateTimeFormatter.ofPattern(timestampFormatPattern).withZone(ZoneId.systemDefault());
		              logger.info(String.format("Defining a Timestamp formatter which convert Instant.now() as %s", formatter.format(Instant.now())));
	              }

                String[] values = new String[meta.getColumnCount()];

                if (csvHeader) {

                    for (int i = 0; i < meta.getColumnCount(); i++) {
                        values[i] = meta.getColumnLabel(i + 1);
                    }

                    csv.writeNext(values, csvQuoteAll);

                }

                long count = 0;

                while (rs.next()) {

                    for (int i = 0; i < values.length; i++) {

                        Object object = rs.getObject(i + 1);

                        String value;
                        if (object == null) {
                        	  value = nullReplacement;
                        } else {
		                        if (formatter!=null && object instanceof Timestamp) {
			                          value = formatter.format(rs.getTimestamp(i+1).toInstant());
		                        } else if (booleanAsInt && object instanceof Boolean) {
		                            value = ((Boolean) object) ? "1" : "0";
		                        } else {
			                          value = object.toString();
		                        }
                        }

                        values[i] = value;

                    }

                    csv.writeNext(values, csvQuoteAll);

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

        if (checksum) {

            String hash = Hex.encodeHexString(digest.digest());

            Path path = Paths.get(out + "." + digest.getAlgorithm().replaceAll("-", "").toLowerCase(Locale.US));

            Files.write(path, hash.getBytes(encoding), CREATE, WRITE, writeMode);

            logger.info(String.format("Generated checksum : %s - %s", path, hash));

        }

    }

    String readText(String text) throws IOException {

        if (text == null) {
            return null;
        }

        Matcher cp = Pattern.compile("^(classpath|cp):(.+)").matcher(text);

        if (cp.matches()) {

            String path = cp.group(2);

            logger.info("Reading text from classpath : " + path);

            byte[] bytes = new byte[4096];

            try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                 InputStream in = Optional.ofNullable(Thread.currentThread()
                         .getContextClassLoader()).orElseGet(Main.class::getClassLoader).getResourceAsStream(path)) {

                for (int i = 0; i != -1; i = in.read(bytes)) {
                    out.write(bytes, 0, i);
                }

                return new String(out.toByteArray(), StandardCharsets.UTF_8);

            }

        }

        Matcher fp = Pattern.compile("^file(path)?:(.+)").matcher(text);

        if (fp.matches()) {

            String path = fp.group(2);

            logger.info("Reading text from filepath : " + path);

            return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);

        }

        return text;

    }

    OutputStream wrapOutput(Path path, OutputStream out) throws IOException {

        String name = path.getFileName().toString();

        if (name.endsWith(".deflate")) {
            return new DeflaterOutputStream(out);
        }

        if (name.endsWith(".gz")) {
            return new GZIPOutputStream(out);
        }

        if (name.endsWith(".bz2")) {
            return new BZip2CompressorOutputStream(out);
        }

        return out;

    }

    boolean shouldFlush(long count, int flush) {
        return flush > 0 && count % flush == 0;
    }

}
