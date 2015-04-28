package io.github.yabench.engines.commons;

import com.google.common.io.Files;

import io.github.yabench.commons.AbstractLauncher;
import io.github.yabench.commons.TemporalGraph;
import io.github.yabench.commons.TemporalRDFReader;
import io.github.yabench.commons.TemporalTriple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.time.Instant;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEngineLauncher extends AbstractLauncher {

	private static final Logger logger = LoggerFactory.getLogger(AbstractEngineLauncher.class);

	private static final String ARG_QUERY = "query";
	private static final String ARG_SOURCE = "source";
	private static final String ARG_DEST = "dest";
	private static final String ARG_HELP = "help";

	private Query query;

	@Override
	public abstract String getName();

	public Query getQuery() {
		return query;
	}

	@Override
	public void launch(String[] args) {
		CommandLineParser parser = new PosixParser();
		Options options = createCLIOptions();
		try {
			CommandLine cli = parser.parse(options, args);

			final String queryString = Files.toString(new File(cli.getOptionValue(ARG_QUERY)), Charset.defaultCharset());
			final File source = new File(cli.getOptionValue(ARG_SOURCE));
			final File dest = new File(cli.getOptionValue(ARG_DEST));
			query = new QueryFactory().create(queryString);

			try (final TemporalRDFReader reader = new TemporalRDFReader(source);
					final Writer writer = new BufferedWriter(new FileWriter(dest))) {
				logger.info("initialize engine");
				EngineFactory engineFactory = new EngineFactory();
				try (Engine engine = engineFactory.create()) {
					engine.initialize();

					final ResultSerializer serializer = new ResultSerializer(writer);

					serializer.initialize();

					TemporalGraph graph;
					long t1 = 0, t2 = 0, time = 0;
					engine.registerQuery(query, serializer);
					logger.info("started sending triples at {}", Instant.now());
					while ((graph = reader.readNextGraph()) != null) {
						if (graph.getTime() == 0)
							t2 = System.nanoTime();

						t1 = (System.nanoTime() - t2) / 1000000;
						Thread.sleep(graph.getTime() - time - t1);
						t2 = System.nanoTime();

						for (TemporalTriple triple : graph.getTriples()) {
							engine.stream(triple.getStatement());
							// logger.info("{} - {}", Instant.now(),
							// triple.getTime());
						}
						time = graph.getTime();
					}

					logger.info("stopped sending triples at {}", Instant.now());

					onInputStreamEnd();

					logger.info("stopped the engine at {}", Instant.now());
				}
			}
		} catch (ParseException ex) {
			printHelp(options, ex.getMessage());
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
		}
	}

	public void onInputStreamEnd() throws InterruptedException {
		// Nothing by default
	}

	private Options createCLIOptions() {
		Options opts = new Options();

		Option query = OptionBuilder.withArgName("query")
				.withDescription("destination file containing the query to be registered at the engine").hasArg().isRequired()
				.create(ARG_QUERY);

		Option dest = OptionBuilder.withArgName("dest").withDescription("destination folder of query results").hasArg().isRequired()
				.create(ARG_DEST);

		Option source = OptionBuilder.withArgName("source").withDescription("source file containing the triples to be streamed").hasArg()
				.isRequired().create(ARG_SOURCE);

		Option help = OptionBuilder.withDescription("print this message").create(ARG_HELP);

		opts.addOption(query);
		opts.addOption(source);
		opts.addOption(dest);
		opts.addOption(help);

		return opts;
	}

}
