package models

//date=201508280242,title=dewalt 20v max 5.0ah li-ion battery DCB205 New. Pay now., link=/building-renovation/tools/power-tools/batteries-chargers/auction-936783439.htm, closingTimeText=closes in 9 mins, bidInfo=, priceInfo=$175.00;
case class Listing(id: String, dayDate: String, title: String, cat: String, link: String, bid: Boolean, priceInfo: Double, closingDate: Long, closingTimeText: String )

case class Transaction(id: String, dayDate: String, mainCat: String, threeLevelCat:String, priceInfo: Double) 
