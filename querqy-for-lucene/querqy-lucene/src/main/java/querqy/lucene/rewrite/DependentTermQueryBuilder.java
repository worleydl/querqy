package querqy.lucene.rewrite;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;
import querqy.lucene.backport.TermStates;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * This {@link TermQueryBuilder} creates a {@link DependentTermQuery}, which takes part in {@link DocumentFrequencyCorrection}
 * and thus depends on other {@link TermQuery}s for scoring.
 */
public class DependentTermQueryBuilder implements TermQueryBuilder {

    protected final DocumentFrequencyCorrection dfc;

    public DependentTermQueryBuilder(final DocumentFrequencyCorrection dfc) {
        this.dfc = dfc;
    }


    @Override
    public Optional<DocumentFrequencyCorrection> getDocumentFrequencyCorrection() {
        return Optional.of(dfc);
    }

    @Override
    public DependentTermQuery createTermQuery(final Term term, final FieldBoost boost) {
        return new DependentTermQuery(term, dfc, boost);
    }

    /**
     * A TermQuery that depends on other term queries for the calculation of the document frequency
     * and/or the boost factor (field weight).
     *
     * @author René Kriegler, @renekrie
     *
     */
    public static class DependentTermQuery extends TermQuery {

        final int tqIndex;
        final DocumentFrequencyCorrection dftcp;
        final FieldBoost fieldBoost;

        public DependentTermQuery(final Term term, final DocumentFrequencyCorrection dftcp,
                                  final FieldBoost fieldBoost) {
            this(term, dftcp, dftcp.termIndex(), fieldBoost);
        }

        protected DependentTermQuery(final Term term, final DocumentFrequencyCorrection dftcp,
                                     final int tqIndex, final FieldBoost fieldBoost) {

            super(term);

            if (fieldBoost == null) {
                throw new IllegalArgumentException("FieldBoost must not be null");
            }

            if (dftcp == null) {
                throw new IllegalArgumentException("DocumentFrequencyAndTermContextProvider must not be null");
            }

            if (term == null) {
                throw new IllegalArgumentException("Term must not be null");
            }

            this.tqIndex  = tqIndex;
            this.dftcp = dftcp;
            this.fieldBoost = fieldBoost;
        }

        @Override
        public Weight createWeight(final IndexSearcher searcher) throws IOException {

            final DocumentFrequencyCorrection.DocumentFrequencyAndTermContext dftc
                    = dftcp.getDocumentFrequencyAndTermContext(tqIndex, searcher.getTopReaderContext());

            if (dftc.df < 1) {
                return new NeverMatchWeight();
            }

            return new TermWeight(searcher, dftc.termStates);

        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = prime  + tqIndex;
            result = prime * result + fieldBoost.hashCode();
           // result = prime * result + getTerm().hashCode(); handled in super class
            return super.hashCode() ^ result;
        }

        @Override
        public boolean equals(final Object obj) {

            if (obj == null) {
                return false;
            }

            if (!(obj instanceof DependentTermQuery)) {
                return false;
            }

            final DependentTermQuery other = (DependentTermQuery) obj;
            final Term term = getTerm();

            if (!term.equals(other.getTerm())) {
                return false;
            }

            if (tqIndex != other.tqIndex)
                return false;

            return fieldBoost.equals(other.fieldBoost);

        }

        @Override
        public String toString(final String field) {
            final Term term = getTerm();
            final StringBuilder buffer = new StringBuilder();
            if (!term.field().equals(field)) {
              buffer.append(term.field());
              buffer.append(":");
            }
            buffer.append(term.text());
            buffer.append(fieldBoost.toString(term.field()));
            return buffer.toString();

        }

        public FieldBoost getFieldBoost() {
            return fieldBoost;
        }


        /**
         * Copied from inner class in {@link TermQuery}
         *
         */
        final class TermWeight extends Weight {
            private final Similarity similarity;
            private final Similarity.SimWeight simWeight;
            private final TermStates termStates;

            public TermWeight(final IndexSearcher searcher, final TermStates termStates) throws IOException {
                super();

                final Term term = getTerm();
                this.termStates = termStates;
                this.similarity = searcher.getSimilarity();

                final long df = termStates.docFreq();

                final CollectionStatistics collectionStats;
                final TermStatistics termStats;

                // TODO: No longer have access to needsScores here, may need to revisit...
                final CollectionStatistics trueCollectionStats = searcher.collectionStatistics(term.field());
                final long maxDoc = Math.max(df, trueCollectionStats.maxDoc());
                final long sumTotalTermFreq = Math.max(trueCollectionStats.sumTotalTermFreq(),
                        termStates.totalTermFreq());

                final long sumDocFreq = Math.max(maxDoc, trueCollectionStats.sumDocFreq());

                collectionStats = new CollectionStatistics(term.field(), maxDoc, maxDoc,
                        Math.max(sumTotalTermFreq, sumDocFreq), sumDocFreq);
                TermContext tctx = TermContext.build(searcher.getTopReaderContext(), term);
                termStats = searcher.termStatistics(term, tctx);


                if (termStats != null) {
                    this.simWeight = null;
                } else {
                    // We've modelled field boosting in a FieldBoost implementation so that for example
                    // field boosts can also depend on the term distribution over fields. Calculate the field boost
                    // using that FieldBoost model and multiply with the general boost
                    final float fieldBoostFactor = fieldBoost.getBoost(term.field(), searcher.getIndexReader());
                    this.simWeight = similarity.computeWeight(
                            fieldBoostFactor,
                            collectionStats,
                            termStats
                    );
                }
            }


            @Override
            public String toString() { return "weight(" + DependentTermQuery.this + ")"; }


            @Override
            public TermScorer scorer(AtomicReaderContext context, boolean inOrder, boolean topScorer, Bits bits) {
                try {
                    // TODO: No built for in Solr4
                    /*
                    assert termStates == null || termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context))
                            : "The top-reader used to create Weight is not the same as the current reader's top-reader ("
                            + ReaderUtil.getTopLevelContext(context);
                     */
                    final TermsEnum termsEnum = getTermsEnum(context);
                    if (termsEnum == null) {
                        return null;
                    }

                    Similarity.SimScorer scorer = similarity.simScorer(this.simWeight, context);
                    // TODO: Revisit logic here, check diff
                    return new TermScorer(this, termsEnum.docs(null, null), scorer);
                } catch (IOException ex) {
                    return null;
                }
            }

            /**
             * Returns a {@link TermsEnum} positioned at this weights Term or null if
             * the term does not exist in the given context
             */
            private TermsEnum getTermsEnum(final AtomicReaderContext context) throws IOException {
                final Term term = getTerm();
                if (termStates != null) {
                    // TODO: Check builtFor assert removal doesn't break anything
                    final TermState state = termStates.get(context);
                    if (state == null) { // term is not present in that reader
                        assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
                        return null;
                    }
                    //System.out.println("LD=" + reader.getLiveDocs() + " set?=" + (reader.getLiveDocs() != null ? reader.getLiveDocs().get(0) : "null"));
                    final TermsEnum termsEnum = context.reader().terms(term.field()).iterator(null);
                    termsEnum.seekExact(term.bytes(), state);
                    return termsEnum;
                } else {
                    // TermQuery used as a filter, so the term states have not been built up front
                    final Terms terms = context.reader().terms(term.field());
                    if (terms == null) {
                        return null;
                    }
                    final TermsEnum termsEnum = terms.iterator(null);
                    if (termsEnum.seekExact(term.bytes())) {
                        return termsEnum;
                    } else {
                        return null;
                    }
                }
            }

            private boolean termNotInReader(final IndexReader reader, final Term term) throws IOException {
                return reader.docFreq(term) == 0;
            }

            @Override
            public Explanation explain(AtomicReaderContext atomicReaderContext, int i) throws IOException {
                return new Explanation(1.0f, "Explain not supported in backport");
            }

            @Override
            public Query getQuery() {
                return null;
            }

            @Override
            public float getValueForNormalization() throws IOException {
                return 0;
            }

            @Override
            public void normalize(float v, float v1) {

            }
        }

        public class NeverMatchWeight extends Weight {

            protected NeverMatchWeight() {
                super();
            }

            @Override
            public Explanation explain(AtomicReaderContext context, int doc) {
                return null;
            }

            @Override
            public Query getQuery() {
                return DependentTermQuery.this;
            }

            @Override
            public float getValueForNormalization() throws IOException {
                return 0;
            }

            @Override
            public void normalize(float v, float v1) {
                // no-op
            }

            @Override
            public Scorer scorer(AtomicReaderContext context, boolean inOrder, boolean topDocs, Bits bits) {
                return null;
            }
        }

    }
}
