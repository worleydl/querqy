package querqy.lucene.rewrite;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Test;

import querqy.lucene.rewrite.DocumentFrequencyAndTermContextProvider.DocumentFrequencyAndTermContext;

import static querqy.lucene.rewrite.TestUtil.addNumDocs;

public class DocumentFrequencyCorrectionTest extends LuceneTestCase {
    
    static final FieldBoost DUMMY_FIELD_BOOST = new ConstantFieldBoost(1f);
    

    @Test
    public void testGetDf() throws Exception {
        
        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, analyzer);
       
        int df = getRandomDf();
        addNumDocs("f1", "a", indexWriter, df);
        
        indexWriter.close();
        
        IndexReader indexReader = DirectoryReader.open(directory); 
        IndexSearcher indexSearcher = newSearcher(indexReader);
        
        DocumentFrequencyCorrection dfc = new DocumentFrequencyCorrection();
        dfc.newClause();
        // the term query registers itself with the dfc
        DependentTermQuery tq = new DependentTermQuery(new Term("f1", "a"), dfc, DUMMY_FIELD_BOOST);
        dfc.finishedUserQuery();
        DocumentFrequencyAndTermContext documentFrequencyAndTermContext
                = dfc.getDocumentFrequencyAndTermContext(tq.tqIndex, indexSearcher);
        
        assertEquals(df, documentFrequencyAndTermContext.termContext.docFreq());
        
        indexReader.close();
        directory.close();
        analyzer.close();
        
        
    }
    
    @Test
    public void testEmptyClauses() throws Exception {
        
        Analyzer analyzer = new MockAnalyzer(random());

        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory, analyzer);
       
        int df1 = getRandomDf();
        addNumDocs("f1", "a", indexWriter, df1);
        
        int df2 = df1 + 5;
        addNumDocs("f1", "b", indexWriter, df2);
        
        indexWriter.close();
        
        
        IndexReader indexReader = DirectoryReader.open(directory); 
        IndexSearcher indexSearcher = newSearcher(indexReader);
        
        DocumentFrequencyCorrection dfc = new DocumentFrequencyCorrection();
        dfc.newClause();
        dfc.newClause();
        
        // the term query registers itself with the dfc
        DependentTermQuery tq1 = new DependentTermQuery(new Term("f1", "a"), dfc, DUMMY_FIELD_BOOST);
        dfc.newClause();
        dfc.newClause();
        DependentTermQuery tq2 = new DependentTermQuery(new Term("f1", "b"), dfc, DUMMY_FIELD_BOOST);
        dfc.newClause();
        dfc.newClause();
        dfc.finishedUserQuery();
        dfc.newClause();
        DependentTermQuery tq1a = new DependentTermQuery(new Term("f1", "a"), dfc, DUMMY_FIELD_BOOST);
        DependentTermQuery tq2a = new DependentTermQuery(new Term("f1", "b"), dfc, DUMMY_FIELD_BOOST);
        dfc.newClause();
        dfc.newClause();
        
        DocumentFrequencyAndTermContext dftc1 = dfc.getDocumentFrequencyAndTermContext(tq1.tqIndex, indexSearcher);
        DocumentFrequencyAndTermContext dftc2 = dfc.getDocumentFrequencyAndTermContext(tq2.tqIndex, indexSearcher);
        DocumentFrequencyAndTermContext dftc1a = dfc.getDocumentFrequencyAndTermContext(tq1a.tqIndex, indexSearcher);
        DocumentFrequencyAndTermContext dftc2a = dfc.getDocumentFrequencyAndTermContext(tq2a.tqIndex, indexSearcher);
        
        assertEquals(df1, dftc1.termContext.docFreq());
        assertEquals(df2, dftc2.termContext.docFreq());
        assertEquals(df2 * 2 - 1, dftc1a.termContext.docFreq()); // df = max in clause + max in user query - 1
        assertEquals(df2 * 2 - 1, dftc2a.termContext.docFreq());
        
        
        indexReader.close();
        directory.close();
        analyzer.close();
        
        
    }
    

    int getRandomDf() {
        return 1 + new Long(Math.round(50.0 * Math.random())).intValue();
    }

}