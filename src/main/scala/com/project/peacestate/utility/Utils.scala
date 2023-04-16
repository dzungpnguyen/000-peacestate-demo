package com.project.peacestate.utility

import com.google.gson.Gson
import scala.util.Random
import com.project.peacestate.model.{DroneReport, Citizen}

object Utils {
  def generateReport(droneId: Int, names: List[String], words: List[String]): DroneReport = {
    val randomLatitude = Random.nextDouble * 90
    val randomLongitude = Random.nextDouble * 180
    val randomCitizens = names
      .map(name => Citizen(name, Random.nextDouble * 100))
      .filter(citizen => Random.nextDouble < 0.5)
    val randomWords = words.filter(word => Random.nextDouble < 0.5)
    DroneReport(droneId, randomLatitude, randomLongitude, randomCitizens, randomWords)
  }

  def toJson(droneReport: DroneReport): String = {
    val citizensAsJson = droneReport.citizens
      .map(citizen =>
        s"""
        {
          "name":"${citizen.name}",
          "peacescore":${citizen.peacescore}
        }
        """)
      .mkString("[", ",", "]")
    val wordsAsJson = droneReport.words
      .map(word => s"""$word""")
      .mkString("[", ",", "]")
    s"""
    {
      "droneId":${droneReport.droneId},
      "latitude":${droneReport.latitude},
      "longitude":${droneReport.longitude},
      "citizens":$citizensAsJson,
      "words":$wordsAsJson
    }
    """
  }

  def fromJson(reportJson: String): DroneReport = {
    val gson = new Gson
    gson.fromJson(reportJson, classOf[DroneReport])
  }
}