/**
 * To run the tests set hadoopHome in executeAndGetOutput() and projectHome
 */
package tj.fs

import org.scalatest.FlatSpec
import org.scalatest.matchers.MustMatchers
import java.io.{File, IOException}
import org.apache.commons.io.IOUtils
import scala.io.Source

class SnackFSShellSpec extends FlatSpec with MustMatchers {

  def executeAndGetOutput(command: Seq[String]): String = {

    val hadoopHome = new File("/opt/dev/hadoop-1.0.4/bin") // to be set

    val builder = new ProcessBuilder(command: _*)
      .directory(hadoopHome)

    val process = builder.start()
    val error = new String(IOUtils.toByteArray(process.getErrorStream))
    val output = IOUtils.readLines(process.getInputStream)
    if (error.length > 0) {
      throw new IOException(error)
    }
    output.toString
  }

  val hadoopFSCommands = Seq("./hadoop", "fs")
  val filesystem = "snackfs://localhost:9000/"

  val testingDir = filesystem + "testFSShell/"
  val projectHome = "/snackfs/src/"          //to be set

  val listCommand = hadoopFSCommands ++ Seq("-lsr", filesystem)

  //mkdir
   it should "make a new directory" in {
     val command = hadoopFSCommands ++ Seq("-mkdir", testingDir)
     val output = executeAndGetOutput(command)
     output must be("[]")
   }

   it should "not make a new directory with the name of an existing one" in {
     val command = hadoopFSCommands ++ Seq("-mkdir", testingDir)
     val exception = intercept[IOException] {
       val output = executeAndGetOutput(command)
     }
     val errMsg = "mkdir: cannot create directory"
     exception.getMessage must startWith(errMsg)
   }

   //copyFromLocal
   it should "copy a file into the filesystem using copyFromLocal" in {
     val source = projectHome + "test/resources/small.txt"
     val command = hadoopFSCommands ++ Seq("-copyFromLocal", source, testingDir)
     val output = executeAndGetOutput(command)
     output must be("[]")

     val listoutPut = executeAndGetOutput(listCommand)
     listoutPut must include("small.txt")
   }

   it should "not overwrite a file into the filesystem using copyFromLocal" in {
     val source = projectHome + "test/resources/small.txt"
     val command = hadoopFSCommands ++ Seq("-copyFromLocal", source, testingDir)
     val exception = intercept[IOException] {
       val output = executeAndGetOutput(command)
     }
     exception.getMessage must include("already exists")
   }

   //copyToLocal
   it should "copy a file from the filesystem using copyToLocal" in {
     val destination = projectHome + "test/resources/TestSmall.txt"
     val source = testingDir + "small.txt"
     val command = hadoopFSCommands ++ Seq("-copyToLocal", source, destination)
     val output = executeAndGetOutput(command)
     output must be("[]")
   }

   it should "not overwrite a file using copyToLocal" in {
     val destination = projectHome + "test/resources/small.txt"
     val source = testingDir + "small.txt"
     val command = hadoopFSCommands ++ Seq("-copyFromLocal", source, destination)
     val exception = intercept[IOException] {
       val output = executeAndGetOutput(command)
     }
     exception.getMessage must include("already exists")
   }

   //cat
   it should "print file content" in {
     val source = projectHome + "test/resources/vsmall.txt"
     val writeCommand = hadoopFSCommands ++ Seq("-copyFromLocal", source, testingDir)
     executeAndGetOutput(writeCommand)
     val readCommand = hadoopFSCommands ++ Seq("-cat", testingDir + "/vsmall.txt")
     val output = executeAndGetOutput(readCommand)
     val fileContent = IOUtils.readLines(Source.fromFile(new File(source)).bufferedReader()).toString
     output must be(fileContent)
   }

   //cp
   it should "copy all files from a directory into another" in {
     val destination = filesystem + "testCpCommand/"
     val source = testingDir
     val command = hadoopFSCommands ++ Seq("-cp", source, destination)
     val output = executeAndGetOutput(command)
     output must be("[]")
   }

  //du
   it should "display aggregate length of files in a directory" in {
     val command = hadoopFSCommands ++ Seq("-du", filesystem)
     val output = executeAndGetOutput(command)
     output must include(filesystem + "testFSShell")
     output must include(filesystem + "testCpCommand")
     output must startWith("[Found 2 items, 598419")
     output must include(filesystem + "testFSShell, 598419")
   }

   it should "display aggregate length of file" in {
     val command = hadoopFSCommands ++ Seq("-du", testingDir + "vsmall.txt")
     val output = executeAndGetOutput(command)
     output must startWith("[Found 1 items, 623 ")
     output must endWith("/testFSShell/vsmall.txt]")
   }

   //dus
   it should "display summary of file lengths" in {
     val command = hadoopFSCommands ++ Seq("-dus", filesystem)
     val output = executeAndGetOutput(command)
     output must startWith("[" + filesystem)
     output must endWith("1196838]")
   }

   //ls
   it should "list children of directory" in {
     val command = hadoopFSCommands ++ Seq("-ls", filesystem)
     val output = executeAndGetOutput(command)
     output must startWith("[Found 2 items,")
     output must include("/testFSShell")
     output must include("/testCpCommand")
   }

   it should "list stats of a file" in {
     val command = hadoopFSCommands ++ Seq("-ls", testingDir + "vsmall.txt")
     val output = executeAndGetOutput(command)
     output must startWith("[Found 1 items,")
     output must include("/testFSShell/vsmall.txt")
   }

   //lsr
   it should "list children of directory recursive" in {
     val output = executeAndGetOutput(listCommand)
     output must include("/testFSShell")
     output must include("/testFSShell/small.txt")
     output must include("/testFSShell/vsmall.txt")
     output must include("/testCpCommand")
     output must include("/testCpCommand/small.txt")
     output must include("/testCpCommand/vsmall.txt")
   }

  /*//moveFromLocal
  it should "result in not implemented" in {
    val source = projectHome + "test/resources/small.txt"
    val command = hadoopFSCommands ++ Seq("-moveFromLocal", source, testingDir)
    val output = executeAndGetOutput(command)
    println(output)
  }*/

  it should "move a file" in {
    val source = testingDir + "small.txt"
    val destination = filesystem + "/small.txt"
    val command = hadoopFSCommands ++ Seq("-mv", source, destination)
    val output = executeAndGetOutput(command)
    println(output)
  }

  //not working
/*
  it should "move multiple files" in {
    val source1 = filesystem + "/testCpCommand/" + "small.txt"
    val source2 = filesystem + "/testCpCommand/" + "vsmall.txt"
    val destination = filesystem + "testMvCommand/"
    val mkdirCommand = hadoopFSCommands ++ Seq("-mkdir", destination)
    executeAndGetOutput(mkdirCommand)
    val command = hadoopFSCommands ++ Seq("-mv", source1,source2, destination)
    val output = executeAndGetOutput(command)
    println(output)
  }*/

  //put (reading from stdin also works)
  it should "copy a file into the filesystem using put" in {
    val source = projectHome + "test/resources/vsmall.txt"
    val command = hadoopFSCommands ++ Seq("-put", source, filesystem)
    executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/vsmall.txt")
  }

  it should "copy multiple files into the filesystem using put" in {
    val source1 = projectHome + "test/resources/small.txt"
    val source2 = projectHome + "test/resources/vsmall.txt"
    val destination = filesystem + "testPutCommand/"
    val mkdirCommand = hadoopFSCommands ++ Seq("-mkdir", destination)
    executeAndGetOutput(mkdirCommand)
    val command = hadoopFSCommands ++ Seq("-put", source1, source2, destination)
    val output = executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/testPutCommand/vsmall.txt")
    listOutPut must include("/testPutCommand/small.txt")
  }

  //stat
  it should "display stat" in {
    val command = hadoopFSCommands ++ Seq("-stat", filesystem)
    val output = executeAndGetOutput(command)
    output must not be("[]")
  }

  //tail
  it should "display last KB of a file" in {
    val source = projectHome + "test/resources/small.txt"
    val readCommand = hadoopFSCommands ++ Seq("-tail", filesystem + "/small.txt")
    val output = executeAndGetOutput(readCommand)
    val fileContent = IOUtils.readLines(Source.fromFile(new File(source)).bufferedReader()).toString
    output.length must be(1024)
    fileContent must include(output)
  }

  //touchz
  it should "create a file of zero length" in{
    val command = hadoopFSCommands ++ Seq("-touchz", filesystem+"emptyFile.txt")
    executeAndGetOutput(command)
    val listOutPut = executeAndGetOutput(listCommand)
    listOutPut must include("/emptyFile.txt")
  }

  //rm
  it should "remove a file" in {
    val command = hadoopFSCommands ++ Seq("-rm", testingDir + "vsmall.txt")
    val output = executeAndGetOutput(command)
    output must startWith("[Deleted")
    output must include("/testFSShell/vsmall.txt")
  }

  //not working
  /*it should "remove an empty directory" in {
    val command = hadoopFSCommands ++ Seq("-rm", testingDir)
    val output = executeAndGetOutput(command)
    output must startWith("[Deleted")
    output must include("/testFSShell")
  }*/

  //rmr
  it should "remove a directory and all its contents" in {
    val command = hadoopFSCommands ++ Seq("-rmr", filesystem+"testPutCommand/")
    val output = executeAndGetOutput(command)
    output must startWith("[Deleted")
    output must include("/testPutCommand")
  }

}
