package io

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.Try

package object mewbase {

  type Result[T] = Either[String, T]

  object AsDate {
    def unapply(dateString: String): Option[LocalDate] =
      Try(LocalDate.parse(dateString, DateTimeFormatter.ISO_DATE)).toOption
  }

}
