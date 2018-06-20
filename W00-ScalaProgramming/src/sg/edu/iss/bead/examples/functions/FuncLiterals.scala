package sg.edu.iss.bead.examples.functions


object ColorPrinter extends App {

  val printerSwitch = false

  def printPages(doc: Document, lastIndex: Int, print: (Int) => Unit) = {  //literal function here : print: (Int) => Unit

    if(lastIndex <= doc.numOfPages && !printerSwitch) for(i <- 1 to lastIndex) print(i)

  }

  val colorPrint = (index: Int) => println(s"Printing Color Page v1 $index.") //version 1

  val colorPrintV2 = new Function1[Int, Unit]{   //version 2
    override def apply(index: Int): Unit =
      println(s"Printing Color Page v2 $index.")
  }

  println("---------Function V1-----------")
  printPages(Document(15, "DOCX"), 2, colorPrint)

  println("---------Function V2-----------")
  printPages(Document(15, "DOCX"), 2, colorPrintV2)

}

case class Document(numOfPages: Int, typeOfDoc: String)



/*
* println("---------Method V3-----------")
  printPages(chapter3.Document(15, "DOCX"), 2, colorPrintV3, !printerSwitch)
*
* */

/*object AClosure extends App {

  var advertisement = "Buy an IPhone7"

  val playingShow = (showName: String) => println(s"Playing $showName. Here's the advertisement: $advertisement")

  playingShow("GOT")

  advertisement = "Buy an IPhone8"

  playingShow("GOF")

}*/
