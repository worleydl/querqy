package querqy.solr;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.DisMaxParams;
import org.apache.solr.common.params.HighlightParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.WrappedQuery;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import querqy.infologging.InfoLogging;
import querqy.model.ExpandedQuery;
import querqy.model.MatchAllQuery;
import querqy.model.Term;
import querqy.parser.WhiteSpaceQuerqyParser;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriteChain;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.SearchEngineRequestAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static querqy.solr.QuerqyDismaxParams.GFB;
import static querqy.solr.QuerqyDismaxParams.GQF;
import static querqy.solr.QuerqyQParserPlugin.PARAM_REWRITERS;
import static querqy.solr.StandaloneSolrTestSupport.withCommonRulesRewriter;
import static querqy.solr.StandaloneSolrTestSupport.withRewriter;

public class QuerqyExpandQParserPluginTest extends SolrTestCaseJ4 {

    public void index() {

        assertU(adoc("id", "1", "f1_stopwords", "a"));
        assertU(adoc("id", "2", "f1_stopwords", "a"));
        assertU(adoc("id", "3", "f2_stopwords", "a"));
        assertU(adoc("id", "4", "f1_stopwords", "b"));
        assertU(adoc("id", "5", "f1_stopwords", "spellcheck", "f2_stopwords", "test"));
        assertU(adoc("id", "6", "f1_stopwords", "spellcheck filtered", "f2_stopwords", "test"));
        assertU(adoc("id", "7", "f1_stopwords", "aaa"));
        assertU(adoc("id", "8", "f1_stopwords", "aaa bbb ccc", "f2_stopwords", "w87"));
        assertU(adoc("id", "9", "f1_stopwords", "ignore o u s"));
        assertU(adoc("id", "10", "f1_stopwords", "vv uu tt ss xx ff gg hh"));
        assertU(adoc("id", "11", "f1_stopwords", "xx yy zz tt ll ff gg hh"));

        assertU(commit());
    }

    @BeforeClass
    public static void beforeTests() throws Exception {
        initCore("solrconfig.xml", "schema-stopwords.xml");
        withCommonRulesRewriter(h.getCore(), "common_rules",
                "configs/commonrules/rules-QuerqyDismaxQParserTest.txt");
        withRewriter(h.getCore(), "match_all_filter", MatchAllRewriter.class);

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clearIndex();
        index();
    }

    @Test
    public void testBasicFunctionality() {
        SolrQueryRequest req = req("q", "f1_stopwords:a^5 f2_stopwords:a^10", "debug", "true", "defType", "querqyex", "isFreeTextSearch", "true", "spellcheck.q", "a");

        assertQ("Basic expansion isn't working!",
             req,"//str[@name='parsedquery' and text()='DisjunctionMaxQuery((f2_stopwords:a^10.0 | f1_stopwords:a^5.0))']");

        req.close();
    }

    @Test
    public void testMultiTerm() {
        SolrQueryRequest req = req("q", "f1_stopwords:(blah)^5 f2_stopwords:(blah)^10", "debug", "true", "defType", "querqyex", "isFreeTextSearch", "true", "spellcheck.q", "a b");

        assertQ("Multiterm query parses",
                req,"//str[@name='parsedquery' and text()='DisjunctionMaxQuery((f2_stopwords:a^10.0 | f1_stopwords:a^5.0)) DisjunctionMaxQuery((f2_stopwords:b^10.0 | f1_stopwords:b^5.0))']");

        req.close();
    }

    @Test
    public void testPassthru() {
        SolrQueryRequest req = req("q", "f1_stopwords:(blah)^5 f2_stopwords:(blah)^10", "debug", "true", "defType", "querqyex", "isFreeTextSearch", "false", "spellcheck.q", "a b");

        assertQ("Multiterm query parses",
                req,"//str[@name='parsedquery' and text()='(+(f1_stopwords:blah^5.0 f2_stopwords:blah^10.0))/no_coord']");

        req.close();
    }

    @Test
    public void testMissingSpellcheck() {
        SolrQueryRequest req = req("q", "f1_stopwords:(blah)^5 f2_stopwords:(blah)^10", "debug", "true", "defType", "querqyex", "isFreeTextSearch", "true");

        assertQEx("Missing spellcheck with isFreeTextSearch set to true should cause exception",
                req,400);

        req.close();
    }

    public static class MatchAllRewriter extends SolrRewriterFactoryAdapter {

        public MatchAllRewriter(final String rewriterId) {
            super(rewriterId);
        }

        @Override
        public void configure(final Map<String, Object> config) { }

        @Override
        public List<String> validateConfiguration(final Map<String, Object> config) {
            return Collections.emptyList();
        }

        @Override
        public RewriterFactory getRewriterFactory() {
            return new RewriterFactory(rewriterId) {
                @Override
                public QueryRewriter createRewriter(final ExpandedQuery input,
                                                    final SearchEngineRequestAdapter searchEngineRequestAdapter) {
                    return query -> {
                        query.setUserQuery(new MatchAllQuery());
                        query.addFilterQuery(WhiteSpaceQuerqyParser.parseString("a"));
                        return query;
                    };
                }

                @Override
                public Set<Term> getCacheableGenerableTerms() {
                    return Collections.emptySet();
                }
            };
        }
    }
}
