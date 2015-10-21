package org.template

import io.prediction.controller.EngineFactory
import io.prediction.controller.Engine
import org.codehaus.jackson.annotate.JsonIgnore
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.map.SerializationConfig.Feature

import scala.beans.BeanProperty
;

// Query most similar (top num) items to the given
case class Query(items: Array[String], num: Int) extends Serializable

case class PredictedResult(itemScores: Array[ItemScore]) extends Serializable

case class SimilarItem(@BeanProperty id:String,  @BeanProperty similarItems:Array[ItemScore]) {

  @JsonIgnore
  def toJson() : String = {
    val mapper:ObjectMapper = new ObjectMapper()
    mapper.disable(Feature.FAIL_ON_EMPTY_BEANS)
    return mapper.writeValueAsString(this)
  }
}

case class ItemScore(@BeanProperty item: String,  @BeanProperty score: Double) extends Serializable with Ordered[ItemScore] {

  def compare(that: ItemScore) = this.score.compare(that.score)

}

object SVDItemSimilarityEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("algo" -> classOf[Algorithm]),
      classOf[Serving])
  }
}