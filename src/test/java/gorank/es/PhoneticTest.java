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
public class PhoneticTest extends ESBaseTest {

    public static final String TEST_PHONETIC_INDEX = "test-phonetic-index";
    public static final String TEST_PHONETIC_TYPE = "test-phonetic-type";
    private static Client client;

    @BeforeClass
    public static void prepare() throws IOException, InterruptedException {
        //client = getTestNodeClient();
        client = getLocalClient();
        createIndexWithSettings(client, TEST_PHONETIC_INDEX, createPhoneticSettings());
        defineCustomMapping(client, TEST_PHONETIC_INDEX, TEST_PHONETIC_TYPE, createMappingBuilder());

        indexDocument(client, TEST_PHONETIC_INDEX, TEST_PHONETIC_TYPE, createSampleDocumentBuilder());
        Thread.sleep(2000);
    }

    private static String createPhoneticSettings() throws IOException {
        return jsonBuilder()
                    .startObject()
                        .startObject("analysis")
                            .startObject("filter")
                                .startObject("dbl_metaphone")
                                    .field("type", "phonetic")
                                    .field("encoder", "double_metaphone")
                                .endObject()
                            .endObject()
                            .startObject("analyzer")
                                .startObject("dbl_metaphone")
                                    .field("tokenizer", "standard")
                                    .field("filter", "dbl_metaphone")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .string();
    }

    private static XContentBuilder createMappingBuilder() throws IOException {
        XContentBuilder mapping = jsonBuilder()
                .startObject()
                    .startObject(TEST_PHONETIC_TYPE)
                        .startObject("properties")
                            .startObject("user")
                                .field("type", "string")
                                .startObject("fields")
                                    .startObject("phonetic")
                                        .field("type", "string")
                                        .field("analyzer", "dbl_metaphone")
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
        deleteIndex(client, TEST_PHONETIC_INDEX);
    }

    protected static XContentBuilder createSampleDocumentBuilder() throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                    .field("user", "johny")
                .endObject();
        return builder;
    }

    @Test
    public void testSimpleQuery(){
        // Check the original word with simple terms query
        SearchResponse response = search("user", "johny");
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void testPhonetic(){
        SearchResponse response = null;

        // Check the original word with simple terms query
        response = search("user.phonetic", "johny");
        Assert.assertEquals(1, response.getHits().totalHits());

        response = search("user.phonetic", "jonny");
        Assert.assertEquals(1, response.getHits().totalHits());

        response = search("user.phonetic", "jonnie");
        Assert.assertEquals(1, response.getHits().totalHits());

        response = search("user.phonetic", "jon");
        Assert.assertEquals(1, response.getHits().totalHits());

        response = search("user.phonetic", "john");
        Assert.assertEquals(1, response.getHits().totalHits());

        response = search("user.phonetic", "walker");
        // He is not there
        Assert.assertEquals(0, response.getHits().totalHits());
    }

    private SearchResponse search(String field, String query) {
        return search(client, TEST_PHONETIC_INDEX, TEST_PHONETIC_TYPE, field, query);
    }
}
