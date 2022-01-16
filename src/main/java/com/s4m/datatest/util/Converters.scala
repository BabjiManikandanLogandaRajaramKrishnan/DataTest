package com.s4m.datatest.util

import java.math.BigInteger
import java.text.{DateFormat, ParseException, SimpleDateFormat}
import java.util.{Calendar, Date, TimeZone}

import com.s4m.datatest.Constants
import org.apache.spark.sql.api.java.UDF2
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

class Converters {

  /**
   * Context Time window calculator
   * @param inputDate
   * @param dateFormat
   * @throws
   * @return
   */
  @throws[ParseException]
  def getTimestamp(inputDate: String, dateFormat: String): String = {

    val inputFormat = new SimpleDateFormat(dateFormat)

    val outputFormat = new SimpleDateFormat(dateFormat)

    val date = inputFormat.parse(inputDate)
    val previousDayCal = Calendar.getInstance
    previousDayCal.setTime(date)
    previousDayCal.add(Calendar.DATE, -1)

    val startDate = outputFormat.format(previousDayCal.getTime)

    startDate
  }

  /**
   * Year, Month, Date parser
   * @param inputDate
   * @param format
   * @throws
   * @return
   */
  @throws[ParseException]
  def getDateParsed(inputDate: String, format: String): String = {
    var formattedDate = ""
    if (inputDate != null) {
      val date = new Date(inputDate)
      val dd = new SimpleDateFormat(format)
      formattedDate = dd.format(date)
    }
    formattedDate
  }

  val convertUDF: UserDefinedFunction = udf((time:String) => {
    getTimestamp(time,dateFormat = Constants.DATE_TIME_FORMAT)
  })

  val parseDate: UserDefinedFunction = udf((inputDate:String, format: String) => {
    getDateParsed(inputDate, format)
  })

}
