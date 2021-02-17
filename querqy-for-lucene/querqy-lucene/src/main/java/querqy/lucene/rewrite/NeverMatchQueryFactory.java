/**
 * 
 */
package querqy.lucene.rewrite;

import org.apache.lucene.search.Query;
import querqy.lucene.backport.MatchNoDocsQuery;

/**
 * @author René Kriegler, @renekrie
 *
 */
public class NeverMatchQueryFactory implements LuceneQueryFactory<Query> {
    
    public static final NeverMatchQueryFactory FACTORY = new NeverMatchQueryFactory();

    @Override
    public void prepareDocumentFrequencyCorrection(final DocumentFrequencyCorrection dfc, final boolean isBelowDMQ) {
        // nothing to do
    }

    @Override
    public Query createQuery(final FieldBoost boostFactor, final float dmqTieBreakerMultiplier,
                             final TermQueryBuilder termQueryBuilder) {
        return new MatchNoDocsQuery();
    }


}
