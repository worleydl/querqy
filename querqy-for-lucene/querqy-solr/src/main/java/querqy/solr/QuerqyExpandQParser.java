package querqy.solr;

import org.apache.commons.lang.StringUtils;
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
import querqy.infologging.InfoLogging;
import querqy.lucene.LuceneQueries;
import querqy.lucene.LuceneSearchEngineRequestAdapter;
import querqy.lucene.QueryParsingController;
import querqy.lucene.rewrite.cache.TermQueryCache;
import querqy.parser.QuerqyParser;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.SearchEngineRequestAdapter;
import querqy.solr.backport.DismaxUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;

public class QuerqyExpandQParser extends QParser {
    private final ExtendedDismaxQParser extendedDismaxQParser;
    private final QuerqyParser querqyParser;

    protected final QueryParsingController controller;
    protected final DismaxSearchEngineRequestAdapter requestAdapter;

    protected LuceneQueries luceneQueries = null;
    protected Query processedQuery = null;


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

        this.querqyParser = querqyParser;

        ModifiableSolrParams mutableParams = new ModifiableSolrParams(params);
        String termText = "";

        if (params.getBool("isFreeTextSearch", false)) {
            this.parseMode = PARSE_MODE.QUERQY;
            extendedDismaxQParser = null;

            Set<String> fields = new HashSet<>();
            List<DismaxUtil.Clause> clauses = DismaxUtil.splitIntoClauses(qstr, false);

            for (DismaxUtil.Clause clause : clauses) {
                if (clause.field != null) {
                    fields.add(clause.field + "^" + clause.boost);
                }

                /**
                 * TODO:
                 * Should you revisit this code you can do some reconstruction of the query using clause.val
                 *
                 * For now since the q had many variations of stemmed/nonstemmed already coming in we're just using
                 * the spellcheck.q
                 */
            }

            // Grab query from spellcheck.q (safer for term text parsing)
            if (params.get("spellcheck.q", "").isEmpty()) {
                throw new SolrException(BAD_REQUEST, "`spellcheck.q` must be set if isFreeTextSearch is true.");
            }

            termText = params.get("spellcheck.q", "*:*");
            mutableParams.set("q", termText);
            mutableParams.set("qf", StringUtils.join(fields, " "));

            requestAdapter = new DismaxSearchEngineRequestAdapter(this, req, termText,
                    SolrParams.wrapDefaults(localParams, mutableParams), this.querqyParser, rewriteChain, infoLogging, termQueryCache);

            // From here we parse like a regular querqy query
            controller = createQueryParsingController();

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
                processedQuery = maybeWrapQuery(luceneQueries.mainQuery);

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
            extendedDismaxQParser.addDebugInfo(debugInfo);
        } else {
            final Map<String, Object> info = controller.getDebugInfo();
            for (final Map.Entry<String, Object> entry : info.entrySet()) {
                debugInfo.add(entry.getKey(), entry.getValue());
            }
        }
    }

      public SearchEngineRequestAdapter getSearchEngineRequestAdapter() {
        return requestAdapter;
    }

    public List<Query> getFilterQueries() {
        return luceneQueries == null ? null : luceneQueries.filterQueries;
    }
}
