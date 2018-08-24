package io.mewbase.projection

import io.mewbase.binders.BinderStore
import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.{Event, EventSource}

package object syntax {

  case class ProjectionName(name: String) {

    def classify(classifier: Event => Option[String]): ProjectionNameAndClassifer =
      ProjectionNameAndClassifer(name, classifier)

  }

  case class ProjectionNameAndClassifer(name: String, classifer: Event => Option[String]) {

    def project(f: (BsonObject, Event) => BsonObject): ProjectionNameClassifierAndFunction =
      ProjectionNameClassifierAndFunction(name, classifer, f)

  }

  case class ProjectionNameClassifierAndFunction(
    name: String,
    classifier: Event => Option[String],
    function: (BsonObject, Event) => BsonObject
  )

  def projection(name: String): ProjectionName =
    ProjectionName(name)

  def projectionManager(eventSource: EventSource, binderStore: BinderStore, projections: ((String, ProjectionNameClassifierAndFunction), String)*): ProjectionManager = {
    val result = ProjectionManager.instance(eventSource, binderStore)
    projections.foreach {
      case ((sourceChannel, ProjectionNameClassifierAndFunction(name, classifier, function)), destinationBinder) =>
        result
          .builder()
          .named(name)
          .projecting(sourceChannel)
          .onto(destinationBinder)
          .filteredBy(e => classifier(e).isDefined)
          .identifiedBy(e => classifier(e).get)
          .as((document, event) => function(document, event))
          .create()
    }
    result
  }

}
