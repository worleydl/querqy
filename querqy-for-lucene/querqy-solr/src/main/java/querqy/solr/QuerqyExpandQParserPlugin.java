package querqy.solr;

import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import querqy.infologging.InfoLogging;
import querqy.lucene.rewrite.cache.TermQueryCache;
import querqy.rewrite.RewriteChain;

/**
 * This parser will accept a query with fields specified and rewrite it as an edismax query with the q
 * only being set to the query text.
 *
 * Presumptions:
 * - All query clauses have fields specified
 * - The query term is the same for all clauses
 */
public class QuerqyExpandQParserPlugin extends QuerqyQParserPlugin {

    public QParser createParser(final String qstr, final SolrParams localParams, final SolrParams params,
                                final SolrQueryRequest req, final RewriteChain rewriteChain,
                                final InfoLogging infoLogging, final TermQueryCache termQueryCache) {
        return new QuerqyExpandQParser(qstr, localParams, params, req,
                createQuerqyParser(qstr, localParams, params, req), rewriteChain, infoLogging, termQueryCache);
    }


}
