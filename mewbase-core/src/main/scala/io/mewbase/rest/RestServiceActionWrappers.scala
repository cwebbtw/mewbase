package io.mewbase.rest

import io.mewbase.binders.BinderStore
import io.mewbase.bson.BsonObject
import io.mewbase.cqrs.{CommandManager, QueryManager}

object RetrieveSingleDocument {

  def apply(binderStore: BinderStore, binderName: String, documentId: String): RestServiceAction.RetrieveSingleDocument =
    RestServiceAction.retrieveSingleDocument(binderStore, binderName, documentId)

}

object ListDocumentIds {

  def apply(binderStore: BinderStore, binderName: String): RestServiceAction.ListDocumentIds =
    RestServiceAction.listDocumentIds(binderStore, binderName)

}

object ExecuteCommand {

  def apply(commandManager: CommandManager, commandName: String, context: BsonObject): RestServiceAction.ExecuteCommand =
    RestServiceAction.executeCommand(commandManager, commandName, context)

}

object RunQuery {

  def apply(queryManager: QueryManager, queryName: String, context: BsonObject): RestServiceAction.RunQuery =
    RestServiceAction.runQuery(queryManager, queryName, context)

}

object ListBinders {

  def apply(binderStore: BinderStore): RestServiceAction.ListBinders =
    RestServiceAction.listBinders(binderStore)

}

object GetMetrics {

  def apply(): RestServiceAction.GetMetrics =
    RestServiceAction.getMetrics

}