package org.template

import java.util

import grizzled.slf4j.Logger
import io.prediction.data.storage.{Storage, elasticsearch}
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._
import org.elasticsearch.spark._

/** Elasticsearch notes:
  * 1) every query clause wil laffect scores unless it has a constant_score and boost: 0
  * 2) the Spark index writer is fast but must assemble all data for the index before the write occurs
  * 3) many operations must be followed by a refresh before the action takes effect--sortof like a transaction commit
  * 4) to use like a DB you must specify that the index of fields are `not_analyzed` so they won't be lowercased,
  *    stemmed, tokenized, etc. Then the values are literal and must match exactly what is in the query (no analyzer)
  */

/** Defines methods to use on Elasticsearch. */
object esClient {
  @transient lazy val logger = Logger[this.type]

  private lazy val client = if (Storage.getConfig("ELASTICSEARCH").nonEmpty)
      new elasticsearch.StorageClient(Storage.getConfig("ELASTICSEARCH").get).client
    else
      throw new IllegalStateException("No Elasticsearch client configuration detected, check your pio-env.sh for" +
        "proper configuration settings")

  // wrong way that uses only default settings, which will be a localhost ES sever.
  //private lazy val client = new elasticsearch.StorageClient(StorageClientConfig()).client

  /** Delete all data from an instance but do not commit it. Until the "refresh" is done on the index
    * the changes will not be reflected.
    * @param indexName will delete all types under this index, types are not used by the UR
    * @param refresh
    * @return true if all is well
    */
  def deleteIndex(indexName: String, refresh: Boolean = false): Boolean = {
    //val debug = client.connectedNodes()
    if (client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()) {
      val delete = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
      if (!delete.isAcknowledged) {
        logger.info(s"Index ${indexName} wasn't deleted, but may have quietly failed.")
      } else {
        // now refresh to get it 'committed'
        // todo: should do this after the new index is created so no index downtime
        if (refresh) refreshIndex(indexName)
      }
      true
    } else {
      logger.warn(s"Elasticsearch index: ${indexName} wasn't deleted because it didn't exist. This may be an error.")
      false
    }
  }

  /** Commits any pending changes to the index */
  def refreshIndex(indexName: String): Unit = {
    client.admin().indices().refresh(new RefreshRequest(indexName)).actionGet()
  }

  /** Create new index and hot-swap the new after it's indexed and ready to take over, then delete the old */
  def hotSwap(alias: String, typeName: String = "items", indexRDD: RDD[scala.collection.Map[String,Any]]): Unit = {
  //def hotSwap(alias: String, typeName: String = "items", indexRDD: RDD[Map[String, Any]]): Unit = {
    // get index for alias, change a char, create new one with new id and index it, swap alias and delete old one
    val aliasMetadata = client.admin().indices().prepareGetAliases(alias).get().getAliases
    val newIndex = alias + "_" + DateTime.now().getMillis.toString
    val newIndexURI = "/" + newIndex + "/" + typeName
    indexRDD.saveToEs(newIndexURI, Map("es.mapping.id" -> "id"))
    //refreshIndex(newIndex)

    if (!aliasMetadata.isEmpty
    && aliasMetadata.get(alias) != null
    && aliasMetadata.get(alias).get(0) != null) { // was alias so remove the old one
      //append the DateTime to the alias to create an index name
      val oldIndex = aliasMetadata.get(alias).get(0).getIndexRouting
      client.admin().indices().prepareAliases()
        .removeAlias(oldIndex, alias)
        .addAlias(newIndex, alias)
        .execute().actionGet()
      deleteIndex(oldIndex) // now can safely delete the old one since it's not used
    } else { // todo: could be more than one index with 'alias' so
      // no alias so add one
      //to clean up any indexes that exist with the alias name
      val indices = util.Arrays.asList(client.admin().indices().prepareGetIndex().get().indices()).get(0)
      if (indices.contains(alias)) {
        //refreshIndex(alias)
        deleteIndex(alias) // index named like the new alias so delete it
      }
      // slight downtime, but only for one case of upgrading the UR engine from v0.1.x to v0.2.0+
      client.admin().indices().prepareAliases()
        .addAlias(newIndex, alias)
        .execute().actionGet()
    }
    // clean out any old indexes that were the product of a failed train?
    val indices = util.Arrays.asList(client.admin().indices().prepareGetIndex().get().indices()).get(0)
    indices.map{ index =>
      if (index.contains(alias) && index != newIndex) deleteIndex(index) //clean out any old orphaned indexes
    }

  }

  /** Performs a search using the JSON query String
    *
    * @param query the JSON query string parable by Elasticsearch
    * @param indexName the index to search
    * @return a [PredictedResults] collection
    */
  def search(query: String, indexName: String): PredictedResult = {
    val sr = client.prepareSearch(indexName).setSource(query).get()

    if (!sr.isTimedOut) {
      val recs = sr.getHits.getHits.map( hit => new ItemScore(hit.getId, hit.getScore.toDouble) )
      logger.info(s"Results: ${sr.getHits.getHits.size} retrieved of " +
        s"a possible ${sr.getHits.totalHits()}")
      new PredictedResult(recs)
    } else {
      logger.info(s"No results for query ${parse(query)}")
      new PredictedResult(Array.empty[ItemScore])
    }

  }

}