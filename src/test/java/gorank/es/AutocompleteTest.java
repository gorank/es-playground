package gorank.es;

import gorank.es.base.ESBaseTest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by goran on 2/28/16.
 */
public class AutocompleteTest extends ESBaseTest {

    public static final String TEST_AC_INDEX = "test-ac-index";
    public static final String TEST_AC_TYPE = "test-ac-type";
    private static Client client;

    @BeforeClass
    public static void prepare() throws IOException, InterruptedException {
        //client = getTestNodeClient();
        client = getLocalClient();
        createIndexWithSettings(client, TEST_AC_INDEX, createAutocompleteSettings());
        defineCustomMapping(client, TEST_AC_INDEX, TEST_AC_TYPE, createMappingBuilder());

        indexDocument(client, TEST_AC_INDEX, TEST_AC_TYPE, createSampleDocumentBuilder());
        Thread.sleep(2000);
    }

    private static String createAutocompleteSettings() throws IOException {
        return jsonBuilder()
                .startObject()
                    .startObject("analysis")
                        .startObject("filter")
                            .startObject("autocomplete_filter")
                                .field("type", "edge_ngram")
                                .field("min_gram", 1)
                                .field("max_gram", 20)
                            .endObject()
                        .endObject()
                        .startObject("analyzer")
                            .startObject("autocomplete")
                                .field("type", "custom")
                                .field("tokenizer", "standard")
                                .field("filter", new String[]{"lowercase", "autocomplete_filter"})
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().string();
    }

    private static XContentBuilder createMappingBuilder() throws IOException {
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                    .startObject(TEST_AC_TYPE)
                        .startObject("properties")
                            .startObject("user")
                                .field("type", "string")
                                .startObject("fields")
                                    .startObject("ac")
                                        .field("type", "string")
                                        .field("analyzer", "autocomplete")
                                        .field("search_analyzer", "standard")
                                    .endObject()
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();
        return mapping;
    }

    @AfterClass
    public static void cleanUp() throws IOException, InterruptedException {
        deleteIndex(client, TEST_AC_INDEX);
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
        SearchResponse response = search("user", "goran");
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void testFullTerm(){
        SearchResponse response = search("user.ac", "goran");
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void testFirstLetter(){
        SearchResponse response = search("user.ac", "g");
        Assert.assertEquals(1, response.getHits().totalHits());

        response = search("user.ac", "ga");
        Assert.assertEquals(0, response.getHits().totalHits());

        response = search("user", "g");
        Assert.assertEquals(0, response.getHits().totalHits());
    }

    private SearchResponse search(String field, String query) {
        return search(client, TEST_AC_INDEX, TEST_AC_TYPE, field, query);
    }
}
