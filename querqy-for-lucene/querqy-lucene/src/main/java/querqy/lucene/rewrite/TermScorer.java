package querqy.lucene.rewrite;

/*
 * Copied from org.apache.lucene.search.TermScorer, which is only package-visible
 */
import java.io.IOException;

import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;

/** Expert: A <code>Scorer</code> for documents matching a <code>Term</code>.
 */
final class TermScorer extends Scorer {
    private final DocsEnum postingsEnum;
    private final Similarity.SimScorer docScorer;

    /**
     * Construct a {@link org.apache.lucene.search.TermScorer} that will iterate all documents.
     */
    TermScorer(Weight weight, DocsEnum postingsEnum, Similarity.SimScorer docScorer) {
        super(weight);
        this.postingsEnum = postingsEnum;
        this.docScorer = docScorer;
    }

    @Override
    public int docID() {
        return postingsEnum.docID();
    }

    @Override
    public int nextDoc() throws IOException {
        return postingsEnum.nextDoc();
    }

    @Override
    public int advance(int i) throws IOException {
        return postingsEnum.advance(i);
    }

    @Override
    public long cost() {
        return 0;
    }

    public final int freq() throws IOException {
        return postingsEnum.freq();
    }

    @Override
    public float score() throws IOException {
        assert docID() != DocIdSetIterator.NO_MORE_DOCS;
        return docScorer.score(postingsEnum.docID(), postingsEnum.freq());
    }

    /** Returns a string representation of this <code>TermScorer</code>. */
    @Override
    public String toString() { return "scorer(" + weight + ")[" + super.toString() + "]"; }
}
