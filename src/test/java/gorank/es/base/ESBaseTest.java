package gorank.es.base;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by goran on 2/28/16.
 */
public class ESBaseTest {

    public static final String TEST_CLUSTER = "test-cluster";

    protected static Client getTestNodeClient(){
        Node node = nodeBuilder().settings(Settings.builder()
                        .put("cluster.name", TEST_CLUSTER)
                        .put("path.home", ".")
                        .build()
            ).node();
        return node.client();
    }

    protected static Client getLocalClient() throws UnknownHostException {
        return TransportClient.builder().build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    }

    protected static void defineCustomMapping(Client client, String indexName, String typeName, XContentBuilder mappingBuilder) throws IOException {
        client.admin().indices()
                .preparePutMapping(indexName).setType(typeName)
                .setSource(mappingBuilder)
                .execute().actionGet();
    }

    protected static void createIndex(Client client, String indexName) {
        client.admin().indices().create(new CreateIndexRequest(indexName)).actionGet();
    }


    protected static void createIndexWithSettings(Client client, String indexName, String settings) throws IOException {
        createIndexWithSettings(client, indexName, Settings.settingsBuilder().loadFromSource(settings));
    }

    protected static void createIndexWithSettings(Client client, String indexName, Settings.Builder settings) throws IOException {
        client.admin().indices().prepareCreate(indexName)
                .setSettings(settings)
                .execute().actionGet();
    }


    protected static void indexDocument(Client client, String indexName, String typeName, XContentBuilder documentBuilder) throws IOException {
        IndexResponse indexResponse = client
                .prepareIndex(indexName, typeName)
                .setSource(documentBuilder)
                .get();
    }

    protected SearchResponse search(Client client, String indexName, String typeName, String field, String query) {
        return search(client, indexName, typeName, QueryBuilders.matchQuery(field, query));
    }

    protected SearchResponse search(Client client, String indexName, String typeName, QueryBuilder query) {
        return client.prepareSearch(indexName).setTypes(typeName)
                .setQuery(query)
                .execute().actionGet();
    }

    protected static void deleteIndex(Client client, String indexName) throws InterruptedException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
        ActionFuture<DeleteIndexResponse> deleteIndexResponseActionFuture = client.admin().indices().delete(deleteIndexRequest);
        try {
            deleteIndexResponseActionFuture.actionGet();
        }catch(IndexNotFoundException e){
            //Ignore, the index is already missing
        }

        Thread.sleep(2000);
    }
}
