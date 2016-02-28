package gorank.es;

import gorank.es.base.ESBaseTest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by goran on 2/28/16.
 */
public class FuzzinessTest extends ESBaseTest {

    public static final String TEST_FUZZY_INDEX = "test-fuzzy-index";
    public static final String TEST_FUZZY_TYPE = "test-fuzzy-type";
    private static Client client;

    @BeforeClass
    public static void prepare() throws IOException, InterruptedException {
        //client = getTestNodeClient();
        client = getLocalClient();

        indexDocument(client, TEST_FUZZY_INDEX, TEST_FUZZY_TYPE, createSampleDocumentBuilder());
        Thread.sleep(2000);
    }

    @AfterClass
    public static void cleanUp() throws IOException, InterruptedException {
        deleteIndex(client, TEST_FUZZY_INDEX);
    }

    protected static XContentBuilder createSampleDocumentBuilder() throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                    .field("user", "goran")
                .endObject();
        return builder;
    }

    @Test
    public void testSimpleQuery(){
        // Check if original word exists with simple query
        SearchResponse response = search(QueryBuilders.matchQuery("user", "goran"));
        // It should be there
        Assert.assertEquals(1, response.getHits().totalHits());

        // Query with missing chars
        response = search(QueryBuilders.matchQuery("user", "gran"));
        // Should not be found
        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void testFuzzinessExact(){
        // This must be found
        SearchResponse response = search(QueryBuilders.matchQuery("user", "goran").fuzziness(1));
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void testFuzzinessMissingChar(){
        // Check with standard match query
        SearchResponse response = search(QueryBuilders.matchQuery("user", "gran"));
        // It should not be found
        Assert.assertEquals(0, response.getHits().totalHits());

        // Check with 1 expansion
        response = search(QueryBuilders.matchQuery("user", "gran").fuzziness(1));
        // It should be there
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void testFuzzinessWrongChar(){
        // Check with wrong char
        SearchResponse response = search(QueryBuilders.matchQuery("user", "gotan"));
        // It should not be found
        Assert.assertEquals(0, response.getHits().totalHits());

        // Check with some fuzziness
        response = search(QueryBuilders.matchQuery("user", "gotan").fuzziness(1));
        // It should be there
        Assert.assertEquals(1, response.getHits().totalHits());

        // Too many mistakes
        response = search(QueryBuilders.matchQuery("user", "gtan").fuzziness(1));
        // It should be there
        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void testFuzzinessMissingFirstChar(){
        // Check with first char missing
        SearchResponse response = search(
                QueryBuilders.matchQuery("user", "oran")
                        .fuzziness(1));
        // Should be found
        Assert.assertEquals(1, response.getHits().totalHits());

        // Expect mismatching, but at least the leading char should be there
        response = search(
                QueryBuilders.matchQuery("user", "oran")
                    .fuzziness(1)
                    .prefixLength(1));
                // Should not be found
        Assert.assertEquals(0, response.getHits().totalHits());
    }

    @Test
    public void testFuzzinessFuzzyQuery(){
        // FuzzyQuery is overriden by match query + fuzziness

        // Check the original word
        SearchResponse response = search(
                QueryBuilders.fuzzyQuery("user", "gran")
                    .maxExpansions(1));
        // Should be found
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    private SearchResponse search(QueryBuilder query) {
        return search(client, TEST_FUZZY_INDEX, TEST_FUZZY_TYPE, query);
    }

}
