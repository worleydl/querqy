package querqy.solr;

import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.function.BoostedQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.ExtendedDismaxQParser;
import org.apache.solr.search.ExtendedQuery;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.search.WrappedQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import querqy.infologging.InfoLogging;
import querqy.lucene.LuceneQueries;
import querqy.lucene.LuceneSearchEngineRequestAdapter;
import querqy.lucene.QueryParsingController;
import querqy.lucene.rewrite.cache.TermQueryCache;
import querqy.model.convert.builder.BooleanQueryBuilder;
import querqy.parser.QuerqyParser;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.SearchEngineRequestAdapter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.Enum.valueOf;
import static java.util.stream.Collectors.joining;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

public class QuerqyExpandQParser extends QParser {
    private final ExtendedDismaxQParser extendedDismaxQParser;
    private final QuerqyParser querqyParser;

    protected final QueryParsingController controller;
    protected final DismaxSearchEngineRequestAdapter requestAdapter;

    protected String parsedQueryString;
    protected String parsedQf;

    protected LuceneQueries luceneQueries = null;
    protected List<Query> querqyBypassQueries;
    protected Set<String> querqyBypassFields;
    protected Query processedQuery = null;

    public final static Logger log = LoggerFactory.getLogger(QuerqyExpandQParser.class);


    private enum PARSE_MODE {
        QUERQY,
        PASSTHRU
    }

    private PARSE_MODE parseMode;

    /**
     * Constructor for the QParser
     *
     * @param qstr        The part of the query string specific to this parser
     * @param localParams The set of parameters that are specific to this QParser.  See http://wiki.apache.org/solr/LocalParams
     * @param params      The rest of the {@link SolrParams}
     * @param req         The original {@link SolrQueryRequest}
     * @param querqyParser The Querqy query parser to be applied to the input query string
     * @param rewriteChain The chain of rewriters to be applied to this request
     * @param infoLogging The info logging object for this request
     * @param termQueryCache The term query cache.
     *
     */
    public QuerqyExpandQParser(final String qstr, final SolrParams localParams, final SolrParams params,
                               final SolrQueryRequest req, final QuerqyParser querqyParser,
                               final RewriteChain rewriteChain, final InfoLogging infoLogging,
                               final TermQueryCache termQueryCache) {
        super(qstr, localParams, params, req);
        final String q = Objects.requireNonNull(qstr).trim();

        if (q.isEmpty()) {
            throw new SolrException(BAD_REQUEST, "Query string must not be empty");
        }

        this.querqyBypassQueries = new LinkedList<>();
        this.querqyBypassFields = new HashSet<>();

        // Customize bypass fields here
        String[] bypassElements = params.get("eqf", "").split(",");
        Arrays.stream(bypassElements).filter( (bypass) -> bypass.length() > 0).forEach( (bypass) -> {
            this.querqyBypassFields.add(bypass);
        });

        this.querqyParser = querqyParser;

        ModifiableSolrParams mutableParams = new ModifiableSolrParams(params);
        String termText = "";

        if (params.getBool("isFreeTextSearch", false)) {
            this.parseMode = PARSE_MODE.QUERQY;
            extendedDismaxQParser = null;

            Map<String, Float> fields = new HighestBoostMap();

            ExtendedDismaxQParser parser = new ExtendedDismaxQParser(qstr, localParams, params, req);
            try {
                Query parsedQuery = parser.parse();

                // Handle the boosted query case (edismax always returns BooleanQuery or BoostedQuery in Solr4)
                if (parsedQuery instanceof BoostedQuery) {
                    parsedQuery = ((BoostedQuery) parsedQuery).getQuery();
                }

                assert parsedQuery instanceof BooleanQuery;
                BooleanQuery bq = (BooleanQuery) parsedQuery;

                descendAndExtract(bq, fields);
            } catch (SyntaxError ex) {
                throw new SolrException(BAD_REQUEST, "Unable to expand query");
            }

            // Grab query from spellcheck.q (safer for term text parsing)
            if (params.get("spellcheck.q", "").isEmpty()) {
                throw new SolrException(BAD_REQUEST, "`spellcheck.q` must be set if isFreeTextSearch is true.");
            }

            termText = params.get("spellcheck.q", "*:*");
            mutableParams.set("q", termText);

            String qf = fields.entrySet()
                    .stream()
                    .map( e -> e.getKey() + "^" + e.getValue())
                    .collect(joining(" "));

            mutableParams.set("qf", qf);
            parsedQueryString = termText;
            parsedQf = qf;

            requestAdapter = new DismaxSearchEngineRequestAdapter(this, req, termText,
                    SolrParams.wrapDefaults(localParams, mutableParams), this.querqyParser, rewriteChain, infoLogging, termQueryCache);

            // From here we parse like a regular querqy query
            controller = createQueryParsingController();
            log.info("qex: " + SolrParams.wrapDefaults(localParams, mutableParams).toString());
        } else {
            parseMode = PARSE_MODE.PASSTHRU;
            extendedDismaxQParser = new ExtendedDismaxQParser(qstr, localParams, params, req);
            controller = null;
            requestAdapter = null;
        }
    }

    public QueryParsingController createQueryParsingController() {
        return new QueryParsingController(requestAdapter);
    }

    @Override
    public Query parse() throws SyntaxError {
        if (parseMode == PARSE_MODE.PASSTHRU) {
            return extendedDismaxQParser.parse();
        } else {
            try {
                luceneQueries = controller.process();
                Query wrappedQuery = maybeWrapQuery(luceneQueries.mainQuery);

                BooleanQuery bq = new BooleanQuery();
                bq.add(new BooleanClause(wrappedQuery, BooleanClause.Occur.SHOULD));
                querqyBypassQueries.stream().forEach( (q) -> {
                    bq.add(new BooleanClause(q, BooleanClause.Occur.SHOULD));
                });

                bq.setMinimumNumberShouldMatch(0);  // Querqy uses internal minmatch, for the top level we use 0
                processedQuery = bq;
            } catch (final LuceneSearchEngineRequestAdapter.SyntaxException e) {
                throw new SyntaxError("Syntax error", e);
            }

            return processedQuery;
        }
    }

    @Override
    public Query getQuery() throws SyntaxError {
        if (parseMode == PARSE_MODE.PASSTHRU) {
            return extendedDismaxQParser.getQuery();
        } else {
            if (query == null) {
                query = parse();
                applyLocalParams();

            }
            return query;
        }
    }

    protected void applyLocalParams() {

        if (localParams != null) {
            final String cacheStr = localParams.get(CommonParams.CACHE);
            if (cacheStr != null) {
                if (CommonParams.FALSE.equals(cacheStr)) {
                    extendedQuery().setCache(false);
                } else if (CommonParams.TRUE.equals(cacheStr)) {
                    extendedQuery().setCache(true);
                } else if ("sep".equals(cacheStr) && !luceneQueries.areQueriesInterdependent) {
                    extendedQuery().setCacheSep(true);
                }
            }

            int cost = localParams.getInt(CommonParams.COST, Integer.MIN_VALUE);
            if (cost != Integer.MIN_VALUE) {
                extendedQuery().setCost(cost);
            }
        }

    }

    private ExtendedQuery extendedQuery() {
        if (query instanceof ExtendedQuery) {
            return (ExtendedQuery) query;
        } else {
            WrappedQuery wq = new WrappedQuery(query);
            wq.setCacheSep(!luceneQueries.areQueriesInterdependent);
            query = wq;
            return wq;
        }
    }

    protected Query maybeWrapQuery(final Query query) {
        if (!luceneQueries.areQueriesInterdependent) {
            if (query instanceof ExtendedQuery) {
                ((ExtendedQuery) query).setCacheSep(false);
                return query;
            } else {
                final WrappedQuery wrappedQuery = new WrappedQuery(query);
                wrappedQuery.setCacheSep(false);
                return wrappedQuery;
            }
        } else {
            return query;
        }
    }

    @Override
    public Query getHighlightQuery() throws SyntaxError {
        if (parseMode == PARSE_MODE.PASSTHRU) {
            return extendedDismaxQParser.getHighlightQuery();
        } else {
            if (processedQuery == null) {
                parse();
            }
            return luceneQueries.userQuery;
        }
    }

    @Override
    public void addDebugInfo(final NamedList<Object> debugInfo) {
        super.addDebugInfo(debugInfo);
        if (parseMode == PARSE_MODE.PASSTHRU) {
            debugInfo.add("querqy_parse_mode", "edismax passthru");
            extendedDismaxQParser.addDebugInfo(debugInfo);
        } else {
            final Map<String, Object> info = controller.getDebugInfo();
            for (final Map.Entry<String, Object> entry : info.entrySet()) {
                debugInfo.add(entry.getKey(), entry.getValue());
            }

            debugInfo.add("querqy_parse_mode", "querqy rules");
            debugInfo.add("querqyex_q", parsedQueryString);
            debugInfo.add("querqyex_qf", parsedQf);
        }
    }

    public SearchEngineRequestAdapter getSearchEngineRequestAdapter() {
        return requestAdapter;
    }

    public List<Query> getFilterQueries() {
        return luceneQueries == null ? null : luceneQueries.filterQueries;
    }

    private void descendAndExtract(BooleanQuery bq, Map<String, Float> fieldsAndBoosts) {
        for (BooleanClause clause : bq.getClauses()) {
            if (clause.getQuery() instanceof BooleanQuery) {
                descendAndExtract((BooleanQuery) clause.getQuery(), fieldsAndBoosts);
            } else {
                Query clauseQuery = clause.getQuery();
                Set<Term> clauseTerms = new HashSet<>();

                if (!(clauseQuery instanceof PrefixQuery)) {
                    clauseQuery.extractTerms(clauseTerms);
                } else {
                    PrefixQuery pQuery = (PrefixQuery) clauseQuery;
                    fieldsAndBoosts.put(pQuery.getField(), pQuery.getBoost());
                }

                for (Term term : clauseTerms) {
                    // Don't add bypass fields
                    if (querqyBypassFields.contains(term.field())) {
                        querqyBypassQueries.add(clauseQuery);
                        break;
                    }

                    fieldsAndBoosts.put(term.field(), clauseQuery.getBoost());
                }
            }
        }
    }

    private static class HighestBoostMap extends HashMap<String, Float> {
        @Override
        public Float put(String key, Float value){
            if (this.containsKey(key)) {
                if (value > this.get(key)) {
                    return super.put(key, value);
                } else {
                    return this.get(key);
                }
            } else {
                return super.put(key, value);
            }
        }
    }
}
