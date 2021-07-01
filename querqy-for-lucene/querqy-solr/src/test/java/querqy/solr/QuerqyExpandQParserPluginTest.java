package querqy.solr;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import querqy.model.ExpandedQuery;
import querqy.model.MatchAllQuery;
import querqy.model.Term;
import querqy.parser.WhiteSpaceQuerqyParser;
import querqy.rewrite.QueryRewriter;
import querqy.rewrite.RewriterFactory;
import querqy.rewrite.SearchEngineRequestAdapter;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static querqy.solr.QuerqyQParserPlugin.PARAM_REWRITERS;
import static querqy.solr.StandaloneSolrTestSupport.withCommonRulesRewriter;
import static querqy.solr.StandaloneSolrTestSupport.withRewriter;

public class QuerqyExpandQParserPluginTest extends SolrTestCaseJ4 {

    public void index() {

        assertU(adoc("id", "1", "f1_stopwords", "a", "f2_stopwords", "a"));
        assertU(adoc("id", "2", "f1_stopwords", "a", "f2_stopwords", "a"));
        assertU(adoc("id", "3", "f1_stopwords", "a", "f2_stopwords", "a"));
        assertU(adoc("id", "4", "f1_stopwords", "b", "f2_stopwords", "a"));
        assertU(adoc("id", "5", "f1_stopwords", "spellcheck", "f2_stopwords", "test"));
        assertU(adoc("id", "6", "f1_stopwords", "spellcheck filtered test pf", "f2_stopwords", "test"));
        assertU(adoc("id", "7", "f1_stopwords", "spellcheck filtered pf"));
        assertU(adoc("id", "8", "f1_stopwords", "aaa"));
        assertU(adoc("id", "9", "f1_stopwords", "aaa bbb ccc", "f2_stopwords", "w87"));
        assertU(adoc("id", "10", "f1_stopwords", "ignore o u s"));
        assertU(adoc("id", "11", "f1_stopwords", "vv uu tt ss xx ff gg hh yy"));
        assertU(adoc("id", "12", "f1_stopwords", "xx yy zz tt ll ff gg hh"));
        assertU(adoc("id", "13", "f1_stopwords", "zz", "f2_stopwords", "filtered"));
        assertU(adoc("id", "14", "f1_stopwords", "zz", "f2_stopwords", "unfiltered"));
        assertU(adoc("id", "15", "f1_stopwords", "abc", "string", "123"));
        assertU(adoc("id", "16", "f1_stopwords", "abc", "string", "456"));
        assertU(adoc("id", "17", "f1_stopwords", "xyz", "string", "789"));

        assertU(commit());
    }

    @BeforeClass
    public static void beforeTests() throws Exception {
        initCore("solrconfig.xml", "schema-stopwords.xml");
        withCommonRulesRewriter(h.getCore(), "common_rules",
                "configs/commonrules/rules-QuerqyExpandQParserTest.txt");
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

        assertQ("Basic expansion is working",
             req,"//str[@name='parsedquery' and text()='DisjunctionMaxQuery((f2_stopwords:a^10.0 | f1_stopwords:a^5.0))']");

        assertQ("Verify parsed q",
                req,"//str[@name='querqyex_q' and text()='a']");

        assertQ("Verify parsed qf",
                req,"//str[@name='querqyex_qf' and text()='f2_stopwords^10.0 f1_stopwords^5.0']");

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
    public void testBooleanLogic() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(xyz) string:(123)",
                "mm", "100",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "xyz 123");


        assertQ("Filter expected",
                req,"//result[@name='response' and @numFound='1']");

        req.close();
    }

    @Test
    public void testPicksHighest() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(zz)^2.0 f1_stopwords:(zz)^4.0",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "zz");

        assertQ("Filter expected",
                req,
                "//str[@name='querqyex_qf' and text()='f1_stopwords^4.0']"
        );

        req.close();
    }


    @Test
    public void testFiltered() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(zz)",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "zz");

        assertQ("Filter expected",
                req,
                "//result[@name='response' and @numFound='1']",
                "//arr[@name='parsed_filter_queries']/str[text() = 'f2_stopwords:filtered']"
        );

        req.close();
    }

    @Test
    public void testFilterPassthru() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:abc",
                "fq", "string:123",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "abc");

        assertQ("FQ passes thru",
                req,
                "//result[@name='response' and @numFound='1']",
                "//arr[@name='parsed_filter_queries']/str[text() = 'string:123']"
        );

        req.close();
    }

    @Test
    public void testDirectFilterPassthru() {
        SolrQueryRequest req = req(
                "q", "pf",
                "fq", "f2_stopwords:(test)",
                "qf", "f1_stopwords",
                "debug", "true",
                "defType", "querqy",
                "spellcheck.q", "pf");

        assertQ("FQ passes thru",
                req,
                "//result[@name='response' and @numFound='1']",
                "//arr[@name='parsed_filter_queries']/str[text() = 'f2_stopwords:test']"
        );

        req.close();
    }

    @Test
    public void testNegateFilterPassthru() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(spellcheck)",
                "fq", "-(f1_stopwords:(pf))",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "spellcheck");

        assertQ("FQ passes thru with negation",
                req,
                "//result[@name='response' and @numFound='1']"
        );

        req.close();
    }

    @Test
    public void testBoostNegateFilterPassthru() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(*)",
                "fq", "-(f2_stopwords:(test))",
                "boost", "sum(5,5)",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "*");

        assertQ("FQ passes thru with negation",
                req,
                "//result[@name='response' and @numFound='14']"
        );

        req.close();
    }

    @Test
    public void testFilterStackPassthru() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(*)",
                "fq", "f1_stopwords:(a)",
                "fq", "f2_stopwords:(a)",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "*");

        assertQ("Multiple fq works",
                req,
                "//result[@name='response' and @numFound='3']"
        );

        req.close();
    }

    @Test
    public void testNumericStringFilterPassthru() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(a)",
                "fq", "-(string:123)",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "a");

        assertQ("Negated numeric string works",
                req,
                "//result[@name='response' and @numFound='3']"
        );

        req.close();
    }

    @Test
    public void testPfPassthru() {
        SolrQueryRequest req = req(
                "q", "f2_stopwords:(test)",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "test pf");

        assertQ("Find lower ID first with no PF",
                req,
                "//result[@name='response' and @numFound='2']",
                "//result/doc[1]/str[@name='id'][text()='5']"
        );

        req.close();

        req = req(
                "q", "f2_stopwords:(test)",
                "pf", "f1_stopwords",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "test pf");

        assertQ("Find higher ID with pf boost applied",
                req,
                "//result[@name='response' and @numFound='2']",
                "//result/doc[1]/str[@name='id'][text()='6']"
        );

        req.close();
    }

    @Test
    public void testWithBoostParam() {
        SolrQueryRequest req = req(
                "q", "f1_stopwords:(zz)^5.0 f2_stopwords:(zz)^10.0",
                "debug", "true",
                "boost", "sum(1,1)",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "zz");

        assertQ("It works with a boost clause",
                req,
                "//result[@name='response' and @numFound='3']",
                "//str[@name='querqyex_qf' and text()='f2_stopwords^10.0 f1_stopwords^5.0']",
                "//str[@name='querqyex_q' and text()='zz']"
        );

        req.close();
    }

    @Test
    public void testQuoteFormat() {
        SolrQueryRequest req = req(
                "q", "(f1_stopwords:\"zz\"^5.0) (f2_stopwords:\"zz\")^10.0",
                "debug", "true",
                PARAM_REWRITERS, "common_rules",
                "defType", "querqyex",
                "isFreeTextSearch", "true",
                "spellcheck.q", "zz");

        assertQ("Parses boosts with quotes",
                req,"//str[@name='querqyex_qf' and text()='f2_stopwords^10.0 f1_stopwords^5.0']");

        req.close();
    }

    @Test
    public void testPassthru() {
        SolrQueryRequest req = req("q", "f1_stopwords:(blah)^5 f2_stopwords:(blah)^10", "debug", "true", "defType", "querqyex", "isFreeTextSearch", "false", "spellcheck.q", "a b");

        assertQ("Multiterm query parses",
                req,"//str[@name='parsedquery' and text()='(+(f1_stopwords:blah^5.0 f2_stopwords:blah^10.0))/no_coord']");

        assertQ("Verify debug",
                req,"//str[@name='querqy_parse_mode' and text()='edismax passthru']");
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
