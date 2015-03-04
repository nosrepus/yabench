package io.github.yabench.streams;

import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.io.IOUtils;
import io.github.yabench.StreamGenerator;
import io.github.yabench.commons.TimeUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.Option;

abstract class AbstractStreamGenerator implements StreamGenerator {

    protected final static String ARG_DURATION = "duration";
    protected final static String DEFAULT_DURATION = "3600000"; //an hour in milliseconds
    private final CommandLine options;
    private final Writer writer;

    protected AbstractStreamGenerator(final Path destination, final CommandLine options)
            throws IOException {
        if (destination == null) {
            this.writer = new OutputStreamWriter(System.out);
        } else {
            this.writer = new FileWriter(destination.toFile());
        }
        this.options = options;
    }

    @Override
    public abstract void generate() throws IOException;

    @Override
    public void close() throws IOException {
        writer.close();
    }

    protected String readTemplate(String name) throws IOException {
        return IOUtils.toString(
                this.getClass().getResourceAsStream("/org/rspbench/tests/" + name));
    }

    protected void writeToDestination(final String str) throws IOException {
        writer.write(str + '\n');
    }

    protected CommandLine getCLIOptions() {
        return options;
    }

    protected static List<Option> getCommonExpectedOptions() {
        List<Option> options = new ArrayList<>();

        options.add(OptionBuilder
                .isRequired()
                .withArgName("milliseconds")
                .withDescription("duration in milliseconds, default: " + DEFAULT_DURATION)
                .hasArg()
                .create(ARG_DURATION));

        return options;
    }

    protected Duration getDuration() {
        return TimeUtils.parseDuration(options.getOptionValue(
                ARG_DURATION, DEFAULT_DURATION));
    }

}
