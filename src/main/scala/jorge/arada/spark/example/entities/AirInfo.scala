package jorge.arada.spark.example.entities

case class AirInfo (airline:String,
                    avail_seat_km_per_week: Long,
                    incidents_00_14: Int,
                    fatal_accidents_00_14: Int,
                    fatalities_00_14: Int,
                    In_top_100_2015: String,
                    rank:Int)
