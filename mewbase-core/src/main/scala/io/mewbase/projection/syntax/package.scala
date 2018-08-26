package io.mewbase.projection

import io.mewbase.Result
import io.mewbase.binders.BinderStore
import io.mewbase.bson.BsonObject
import io.mewbase.eventsource.{Event, EventSource}

package object syntax {

  case class ProjectionName(name: String) {

    def classify(classifier: Event => Result[String]): ProjectionNameAndClassifer =
      ProjectionNameAndClassifer(name, classifier)

  }

  case class ProjectionNameAndClassifer(name: String, classifer: Event => Result[String]) {

    def project(f: (BsonObject, Event) => Result[BsonObject]): ProjectionNameClassifierAndFunction =
      ProjectionNameClassifierAndFunction(name, classifer, f)

  }

  case class ProjectionNameClassifierAndFunction(
    name: String,
    classifier: Event => Result[String],
    function: (BsonObject, Event) => Result[BsonObject]
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
          .filteredBy(e => classifier(e).isRight)
          .identifiedBy(e => classifier(e).right.get)
          .as((document, event) => {
            val result = function(document, event)
            result match {
              case Right(value) => value
              case Left(reason) =>
                throw new IllegalStateException(s"projection '$name' failed because: $reason")
            }
          })
          .create()
    }
    result
  }

}
