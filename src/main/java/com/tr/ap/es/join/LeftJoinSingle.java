package com.tr.ap.es.join;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.get.GetField;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.script.AbstractSearchScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.NativeScriptFactory;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.search.SearchHit;

//TODO rename to inner_join_single
public class LeftJoinSingle extends AbstractSearchScript
{
    private final String                indexName;
    private final String                typeName;
    private final String                joinFieldLeft;
    private final String                joinFieldRight;
    private final String                field;

    private final Client                client;
    private final ESLogger              logger;
    private final Cache<Object, Object> cache;

    public LeftJoinSingle(String indexName, String typeName, String joinFieldLeft, String joinFieldRight,
            String field, Client client, ESLogger logger, Cache<Object, Object> cache)
    {
        super();
        this.indexName = indexName;
        this.typeName = typeName;
        this.joinFieldLeft = joinFieldLeft;
        this.joinFieldRight = joinFieldRight;
        this.field = field;
        this.client = client;
        this.logger = logger;
        this.cache = cache;
    }

    @Override
    public Object run()
    {
        /**
         * TODO With elastic 1.3 we can actually store queries by name in a
         * special index. Then this becomes simply template name and list of
         * params
         */

        logger.info("Join request: indexName={}, typeName={}, joinFieldLeft={}, joinFieldRight={}, field={}",
                indexName, typeName, joinFieldLeft, joinFieldRight, field);
        List<?> values = ((ScriptDocValues) doc().get(joinFieldLeft)).getValues();
        if (values.isEmpty())
        {
         // nothing to search for
            logger.info("Join field is empty");
            return null; 
        }

        final Object joinFieldValue = values.get(0);
        final String cacheKey = new StringBuilder().append(indexName).append(typeName).append(joinFieldRight)
                .append(joinFieldValue).toString();
        Object fieldValue = cache.getIfPresent(cacheKey);

        if (fieldValue != null)
        {
            logger.info("Found in cache: {}",fieldValue);
            return fieldValue;
        }

        if ("_id".equals(joinFieldRight))
        {
            logger.info("Right-side field is _id, using Get instead");
            GetResponse getResponse = client.prepareGet(indexName, typeName, joinFieldValue.toString())
                    .setFields(field).get();
            GetField getField = null;
            if (getResponse.isExists() && (getField = getResponse.getField(field)) != null)
            {
                cache.put(cacheKey, getField.getValue());
                return getField.getValue();
            }
            else
            {
                return null;
            }
        }
        else
        {
            logger.info("Not in cache, searching index");
            final QueryBuilder builder = QueryBuilders.termQuery(joinFieldRight, joinFieldValue);
            final SearchRequestBuilder searchBuilder = client.prepareSearch().setQuery(builder).addField(field)
                    .setSize(1);
            if (indexName != null)
            {
                // TODO support multiple indices
                searchBuilder.setIndices(indexName);
            }
            if (typeName != null)
            {
                // TODO support multiple types
                searchBuilder.setTypes(typeName);
            }
            // TODO support indicesOptions
            // searchBuilder.setIndicesOptions(indicesOptions)

            // TODO support size parameter

            final SearchResponse response = searchBuilder.get();
            final SearchHit[] hit = response.getHits().getHits();
            if (hit.length > 0)
            {
                cache.put(cacheKey, hit[0].field(field).getValue());
                return hit[0].field(field).getValue();
            }
            else
            {
                return null;
            }
        }
    }

    // TODO Pull from Lookup script online as well

    public static class Factory extends AbstractComponent implements NativeScriptFactory
    {
        private Cache<Object, Object> cache;
        private final Node            node;

        @Inject
        public Factory(Node node, Settings settings)
        {
            super(settings);
            this.node = node;
            // TODO Use CacheLoader instead
            cache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.MINUTES).build();
            node.client().prepareSearch("").setq
            
        }

        @Override
        public ExecutableScript newScript(@Nullable Map<String, Object> params)
        {
            if (params == null)
            {
                throw new ScriptException("Missing params");
            }
            // TODO some params are required

            // TODO allow to avoid cache

            return new LeftJoinSingle((String) params.get("_index"), (String) params.get("_type"),
                    (String) params.get("joinFieldLeft"), (String) params.get("joinFieldRight"),
                    (String) params.get("field"), node.client(), logger, cache);
        }

    }
}
