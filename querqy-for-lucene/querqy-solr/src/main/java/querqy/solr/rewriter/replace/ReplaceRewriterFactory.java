package querqy.solr.rewriter.replace;

import org.apache.solr.common.SolrException;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.commonrules.QuerqyParserFactory;
import querqy.rewrite.commonrules.WhiteSpaceQuerqyParserFactory;
import querqy.solr.utils.ConfigUtils;
import querqy.solr.SolrRewriterFactoryAdapter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReplaceRewriterFactory extends SolrRewriterFactoryAdapter {

    public static final String KEY_CONFIG_RULES = "rules";
    public static final String KEY_CONFIG_RHS_QUERY_PARSER = "querqyParser";
    public static final String KEY_CONFIG_INPUT_DELIMITER = "inputDelimiter";
    public static final String KEY_CONFIG_IGNORE_CASE = "ignoreCase";


    private static final Boolean DEFAULT_IGNORE_CASE = true;
    private static final String DEFAULT_INPUT_DELIMITER = "\t";
    private static final QuerqyParserFactory DEFAULT_RHS_QUERY_PARSER = new WhiteSpaceQuerqyParserFactory();

    private querqy.rewrite.contrib.ReplaceRewriterFactory delegate = null;

    public ReplaceRewriterFactory(final String rewriterId) {
        super(rewriterId);
    }

    @Override
    public void configure(final Map<String, Object> config) {
        final String rules = (String) config.get(KEY_CONFIG_RULES);
        final InputStreamReader rulesReader = new InputStreamReader(new ByteArrayInputStream(rules.getBytes()));

        final boolean ignoreCase = ConfigUtils.getArg(config, KEY_CONFIG_IGNORE_CASE, DEFAULT_IGNORE_CASE);

        final String inputDelimiter = ConfigUtils.getArg(config, KEY_CONFIG_INPUT_DELIMITER, DEFAULT_INPUT_DELIMITER);

        final QuerqyParserFactory querqyParser = ConfigUtils.getInstanceFromArg(
                config, KEY_CONFIG_RHS_QUERY_PARSER, DEFAULT_RHS_QUERY_PARSER);

        try {
            delegate = new querqy.rewrite.contrib.ReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase,
                    inputDelimiter, querqyParser.createParser());
        } catch (final IOException e) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cannot parse previously validated " +
                    "configuration for rewriter " + rewriterId, e);
        }
    }

    @Override
    public List<String> validateConfiguration(final Map<String, Object> config) {
        final String rules = (String) config.get(KEY_CONFIG_RULES);
        if (rules == null) {
            return Collections.singletonList("Property '" + KEY_CONFIG_RULES + "' not configured");
        }

        final InputStreamReader rulesReader = new InputStreamReader(new ByteArrayInputStream(rules.getBytes()));

        final boolean ignoreCase = ConfigUtils.getArg(config, KEY_CONFIG_IGNORE_CASE, DEFAULT_IGNORE_CASE);

        final String inputDelimiter = ConfigUtils.getArg(config, KEY_CONFIG_INPUT_DELIMITER, DEFAULT_INPUT_DELIMITER);


        final QuerqyParserFactory querqyParser;
        try {
            querqyParser = ConfigUtils.getInstanceFromArg(config, KEY_CONFIG_RHS_QUERY_PARSER, DEFAULT_RHS_QUERY_PARSER);
        } catch (final Exception e) {
            return Collections.singletonList("Invalid attribute '" + KEY_CONFIG_RHS_QUERY_PARSER + "': " + e.getMessage());
        }

        try {
            new querqy.rewrite.contrib.ReplaceRewriterFactory(rewriterId, rulesReader, ignoreCase,
                    inputDelimiter, querqyParser.createParser());
        } catch (final IOException e) {
            return Collections.singletonList("Cannot create rewriter: " + e.getMessage());
        }

        return null;
    }

    @Override
    public RewriterFactory getRewriterFactory() {
        return delegate;
    }
}