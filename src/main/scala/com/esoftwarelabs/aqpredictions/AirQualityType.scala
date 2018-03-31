package com.esoftwarelabs.aqpredictions

object AirQualityType {
  sealed trait AirQualityType {
    def reading: Int
    def name: String
  }

  case object CLEANAIR extends AirQualityType {
    val reading = 0
    val name = "CleanAir"
  }

  case object POLLUTION extends AirQualityType {
    val reading = 1
    val name = "Pollution"
  }

  case object ALCOHOL extends AirQualityType {
    val reading = 2
    val name = "Alcohol"
  }

  case object FLAMMABLE extends AirQualityType {
    val reading = 3
    val name = "Flammable"
  }

    
    def fromPrediction(num: Int): String = num match {
      case CLEANAIR.reading => CLEANAIR.name
      case POLLUTION.reading => POLLUTION.name
      case ALCOHOL.reading => ALCOHOL.name
      case FLAMMABLE.reading => FLAMMABLE.name
      case _ => "!!ERROR!! Not defined"
    }
    
}