/*
 * Licensed to Tuplejump Software Pvt. Ltd. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Tuplejump Software Pvt. Ltd. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.hadoop.fs

import org.apache.hadoop.fs.shell.{CommandFormat, Count}
import org.apache.hadoop.util.ToolRunner
import org.apache.hadoop.ipc.{RemoteException, RPC}
import java.io.{FileNotFoundException, IOException}
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils


class SnackFSShell extends FsShell {

  val dateForm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  private def printUsage(cmd: String) = {
    val notSupported: String = "This command is not supported"
    val prefix: String = "Usage: java " + classOf[SnackFSShell].getSimpleName
    if (cmd.isEmpty) {
      System.err.println(prefix)
      val commandList = """|           [-ls <path>]
                          |           [-lsr <path>]
                          |           [-df [<path>]]
                          |           [-du <path>]
                          |           [-dus <path>]
                          |           [-count[-q] <path>]
                          |           [-mv <src> <dst>]
                          |           [-cp <src> <dst>]
                          |           [-rm <path>]
                          |           [-rmr <path>]
                          |           [-put <localsrc> ... <dst>]
                          |           [-copyFromLocal <localsrc> ... <dst>]
                          |           [-get [-ignoreCrc] [-crc] <src> <localdst>]
                          |           [-getmerge <src> <localdst> [addnl]]
                          |           [-cat <src>]
                          |           [-text <src>]
                          |           [-copyToLocal [-ignoreCrc] [-crc] <src> <localdst>]
                          |           [-mkdir <path>]
                          |           [-touchz <path>]
                          |           [-test -[ezd] <path>]
                          |           [-stat [format] <path>]
                          |           [-tail <src>]
                          |           [-help [cmd]]
                          | """
      System.err.println(commandList.stripMargin)
    } else {
      cmd.tail match {
        case "ls" | "lsr" | "du" | "dus" | "touchz" | "mkdir" | "text" =>
          System.err.println(prefix + " [" + cmd + " <path>]")
        case "count" =>
          System.err.println(prefix + " [" + Count.USAGE + "]")
        case "rm" | "rmr" | "cat" | "tail" =>
          System.err.println(prefix + " [" + cmd + " <src>]")
        case "mv" | "cp" =>
          System.err.println(prefix + " [" + cmd + " <src> <dst>]")
        case "put" | "copyFromLocal" =>
          System.err.println(prefix + " [" + cmd + " <localsrc> ... <dst>]")
        case "get" | "copyToLocal" =>
          System.err.println(prefix + " [" + cmd + " [-ignoreCrc] [-crc] <src> <localdst>]")
        case "test" =>
          System.err.println(prefix + " [-test -[ezd] <path>]")
        case "stat" =>
          System.err.println(prefix + " [-stat [format] <path>]")
        case _ =>
          System.err.println(notSupported)
      }
    }
  }

  private def getCommandGlossary: Map[String, String] = {
    val lsHelp: String = """|-ls <path>: 	List the contents that match the specified file pattern. If
                           |		path is not specified, the contents of /user/<currentUser>
                           |		will be listed. Directory entries are of the form
                           |			dirName (full path) <dir>
                           |		and file entries are of the form
                           |			fileName(full path) <r n> size
                           |		where n is the number of replicas specified for the file
                           |		and size is the size of the file, in bytes."""

    val lsrHelp: String = """|-lsr <path>: 	Recursively list the contents that match the specified
                            |		file pattern.  Behaves similar to snackfs -ls,
                            |		except that the data is shown for all the entries in the
                            |		subtree.
                            | """

    val duHelp: String = """|-du <path>: 	Show the amount of space, in bytes, used by the files that
                           |		match the specified file pattern.  Equivalent to the unix
                           |		command "du -sb <path>/*" in case of a directory,
                           |		and to "du -b <path>" in case of a file.
                           |		The output is in the form
                           |			name(full path) size (in bytes)"""

    val dusHelp: String = """|-dus <path>: 	Show the amount of space, in bytes, used by the files that
                            |		match the specified file pattern.  Equivalent to the unix
                            |		command "du -sb"  The output is in the form
                            |			name(full path) size (in bytes)
                            | """

    val mvHelp: String = """|-mv <src> <dst>:   Move files that match the specified file pattern <src>
                           |		to a destination <dst>.  When moving multiple files, the
                           |		destination must be a directory."""

    val cpHelp: String = """|-cp <src> <dst>:   Copy files that match the file pattern <src> to a
                           |		destination.  When copying multiple files, the destination
                           |		must be a directory. """

    val rmHelp: String = """|-rm <src>: 	Delete all files that match the specified file pattern.
                           |		Equivalent to the Unix command "rm <src>"
                           | """

    val rmrHelp: String = """|-rmr <src>: 	Remove all directories which match the specified file
                            |		pattern. Equivalent to the Unix command "rm -rf <src>"
                            | """

    val putHelp: String = """|-put <localsrc> ... <dst>: 	Copy files from the local file system
                            |		into fs.
                            | """

    val copyFromLocalHelp: String = """|-copyFromLocal <localsrc> ... <dst>: Identical to the -put command.
                                      | """

    val getHelp: String = """|-get [-ignoreCrc] [-crc] <src> <localdst>:  Copy files that match the file pattern <src>
                            |		to the local name.  <src> is kept.  When copying mutiple,
                            |		files, the destination must be a directory.
                            | """

    val getmergeHelp: String = """|-getmerge <src> <localdst>:  Get all the files in the directories that
                                 |		match the source file pattern and merge and sort them to only
                                 |		one file on local fs. <src> is kept.
                                 | """

    val catHelp: String = """|-cat <src>: 	Fetch all files that match the file pattern <src>
                            |		and display their content on stdout.
                            | """

    val textHelp: String = """|-text <src>: 	Takes a source file and outputs the file in text format.
                             |		The allowed formats are zip and TextRecordInputStream.
                             | """

    val copyToLocalHelp: String = """|-copyToLocal [-ignoreCrc] [-crc] <src> <localdst>:  Identical to the -get command.
                                    | """

    val mkdirHelp: String = """|-mkdir <path>: 	Create a directory in specified location.
                              | """

    val touchzHelp: String = """|-touchz <path>: Write a timestamp in yyyy-MM-dd HH:mm:ss format
                               |		in a file at <path>. An error is returned if the file exists with non-zero length
                               | """

    val testHelp: String = """|-test -[ezd] <path>: If file exists,or has zero length, or is a directory
                             |		then return 0, else return 1.
                             | """

    val statHelp: String = """|-stat [format] <path>: Print statistics about the file/directory at <path>
                             |		in the specified format. Format accepts filesize in blocks (%b), filename (%n),
                             |		block size (%o), replication (%r), modification date (%y, %Y)
                             | """

    val tailHelp: String = """|-tail <file>:  Show the last 1KB of the file.
                             | """

    /*val chmod: String = FsShellPermissions.CHMOD_USAGE + "\n" + "\t\tChanges permissions of a file.\n" + "\t\tThis works similar to shell's chmod with a few exceptions.\n\n" + "\t-R\tmodifies the files recursively. This is the only option\n" + "\t\tcurrently supported.\n\n" + "\tMODE\tMode is same as mode used for chmod shell command.\n" + "\t\tOnly letters recognized are 'rwxX'. E.g. a+r,g-w,+rwx,o=r\n\n" + "\tOCTALMODE Mode specifed in 3 digits. Unlike shell command,\n" + "\t\tthis requires all three digits.\n" + "\t\tE.g. 754 is same as u=rwx,g=rx,o=r\n\n" + "\t\tIf none of 'augo' is specified, 'a' is assumed and unlike\n" + "\t\tshell command, no umask is applied.\n"
    val chown: String = FsShellPermissions.CHOWN_USAGE + "\n" + "\t\tChanges owner and group of a file.\n" + "\t\tThis is similar to shell's chown with a few exceptions.\n\n" + "\t-R\tmodifies the files recursively. This is the only option\n" + "\t\tcurrently supported.\n\n" + "\t\tIf only owner or group is specified then only owner or\n" + "\t\tgroup is modified.\n\n" + "\t\tThe owner and group names may only cosists of digits, alphabet,\n" + "\t\tand any of '-_.@/' i.e. [-_.@/a-zA-Z0-9]. The names are case\n" + "\t\tsensitive.\n\n" + "\t\tWARNING: Avoid using '.' to separate user name and group though\n" + "\t\tLinux allows it. If user names have dots in them and you are\n" + "\t\tusing local file system, you might see surprising results since\n" + "\t\tshell command 'chown' is used for local files.\n"
    val chgrp: String = FsShellPermissions.CHGRP_USAGE + "\n" + "\t\tThis is equivalent to -chown ... :GROUP ...\n"*/

    val helpHelp: String = """|-help [cmd]: 	Displays help for given command or all commands if none
                             |		is specified.
                             | """

    val cmdHelpMap = Map(
      "ls" -> lsHelp,
      "lsr" -> lsrHelp,
      "du" -> duHelp,
      "dus" -> dusHelp,
      "mv" -> mvHelp,
      "cp" -> cpHelp,
      "rm" -> rmHelp,
      "rmr" -> rmrHelp,
      "put" -> putHelp,
      "copyFromLocal" -> copyFromLocalHelp,
      "get" -> getHelp,
      "getmerge" -> getmergeHelp,
      "cat" -> catHelp,
      "text" -> textHelp,
      "copyToLocal" -> copyToLocalHelp,
      "mkdir" -> mkdirHelp,
      "touchz" -> touchzHelp,
      "test" -> testHelp,
      "stat" -> statHelp,
      "tail" -> tailHelp,
      "help" -> helpHelp,
      "count" -> Count.DESCRIPTION
    )
    cmdHelpMap
  }

  private def printHelp(cmd: String) = {
    val notSupported: String = "This command is not supported"

    val glossary = getCommandGlossary
    if (cmd.trim.isEmpty) {
      glossary.foreach {
        case (command: String, help: String) => println(help.stripMargin)
      }
    } else if (glossary.keys.toList.contains(cmd)) {
      println(glossary(cmd).stripMargin)
    } else {
      println(notSupported)
    }
  }

  /**
   * method to return an array of paths for all the arguments except first and last
   * @param params
   * @return
   */
  private def copyReqdArgs(params: Array[String]): Array[Path] = {
    val srcs: Array[Path] = params.slice(1, params.length - 1).map(arg => new Path(arg))
    srcs
  }

  /** helper returns listStatus() */
  private def shellListStatus(cmd: String, srcFs: FileSystem, src: FileStatus): Array[FileStatus] = {
    if (!src.isDir) {
      val files: Array[FileStatus] = Array(src)
      files
    } else {
      val path: Path = src.getPath
      try {
        val files: Array[FileStatus] = srcFs.listStatus(path)
        if (files == null) {
          System.err.println(cmd + ": could not get listing for '" + path + "'")
        }
        files
      }
      catch {
        case e: IOException =>
          System.err.println(cmd + ": could not get get listing for '" + path + "' : " + e.getMessage.split("\n")(0))
          null
      }
    }
  }

  /**
   * Get a listing of all files in that match the file pattern <i>srcf</i>.
   * @param srcf a file pattern specifying source files
   * @param recursive if need to list files in subdirs
   * @throws IOException
   * @see org.apache.hadoop.fs.FileSystem#globStatus(Path)
   */
  private def ls(srcf: String, recursive: Boolean): Int = {
    val srcPath: Path = new Path(srcf)
    val srcFs: FileSystem = srcPath.getFileSystem(this.getConf)
    val srcs: Array[FileStatus] = srcFs.globStatus(srcPath)
    if (srcs == null || srcs.length == 0) {
      throw new FileNotFoundException("Cannot access " + srcf + ": No such file or directory.")
    }
    val printHeader: Boolean = srcs.length == 1
    var numOfErrors: Int = 0
    srcs.foreach(src => numOfErrors += ls(src, srcFs, recursive, printHeader))
    if (numOfErrors == 0) 0 else -1
  }

  private def ls(src: FileStatus, srcFs: FileSystem, recursive: Boolean, printHeader: Boolean): Int = {
    val cmd: String = if (recursive) "lsr" else "ls"
    val items: Array[FileStatus] = shellListStatus(cmd, srcFs, src)
    if (items == null) {
      1
    }
    else {
      var numOfErrors: Int = 0
      if (!recursive && printHeader) {
        if (items.length != 0) {
          System.out.println("Found " + items.length + " items")
        }
      }

      var maxReplication: Int = 3
      var maxLen: Int = 10
      var maxOwner: Int = 0
      var maxGroup: Int = 0

      items.foreach(item => {
        val stat: FileStatus = item
        val replication: Int = String.valueOf(stat.getReplication).length
        val len: Int = String.valueOf(stat.getLen).length
        val owner: Int = String.valueOf(stat.getOwner).length
        val group: Int = String.valueOf(stat.getGroup).length
        if (replication > maxReplication) maxReplication = replication
        if (len > maxLen) maxLen = len
        if (owner > maxOwner) maxOwner = owner
        if (group > maxGroup) maxGroup = group
      })

      items.foreach(item => {
        val stat: FileStatus = item
        val cur: Path = stat.getPath
        val mdate: String = dateForm.format(new Date(stat.getModificationTime))

        System.out.print((if (stat.isDir) "d" else "-") + stat.getPermission + " ")
        System.out.printf("%" + maxReplication + "s ", if (!stat.isDir) stat.getReplication.toString else "-")

        if (maxOwner > 0) System.out.printf("%-" + maxOwner + "s ", stat.getOwner)
        if (maxGroup > 0) System.out.printf("%-" + maxGroup + "s ", stat.getGroup)

        val fileSize = ("%" + maxLen + "d").format(stat.getLen)
        print(fileSize)

        System.out.print(mdate + " ")
        System.out.println(cur.toUri.getPath)

        if (recursive && stat.isDir) {
          numOfErrors += ls(stat, srcFs, recursive, printHeader)
        }
      })

      numOfErrors
    }
  }

  /**
   * Apply operation specified by 'cmd' on all parameters
   * starting from argv[startindex].
   */
  private def doall(cmd: String, argv: Array[String], startindex: Int): Int = {

    var exitCode: Int = 0
    var i: Int = startindex
    val rmSkipTrash: Boolean = true

    val params = argv.slice(i, argv.length)

    params.foreach(arg => {
      try {
        if ("-cat" == cmd) {
          cat(argv(i), true)
        }
        else if ("-mkdir" == cmd) {
          mkdir(argv(i))
        }
        else if ("-rm" == cmd) {
          delete(argv(i), false, rmSkipTrash)
        }
        else if ("-rmr" == cmd) {
          delete(argv(i), true, rmSkipTrash)
        }
        else if ("-du" == cmd) {
          du(argv(i))
        }
        else if ("-dus" == cmd) {
          dus(argv(i))
        }
        else if (Count.matches(cmd)) {
          new Count(argv, i, getConf).runAll
        }
        else if ("-ls" == cmd) {
          exitCode = ls(argv(i), recursive = false)
        }
        else if ("-lsr" == cmd) {
          exitCode = ls(argv(i), recursive = true)
        }
        else if ("-touchz" == cmd) {
          touchz(argv(i))
        }
        else if ("-text" == cmd) {
          text(argv(i))
        }
      }
      catch {
        case e: RemoteException =>
          exitCode = -1
          try {
            val content: Array[String] = e.getLocalizedMessage.split("\n")
            System.err.println(cmd.substring(1) + ": " + content(0))
          }
          catch {
            case ex: Exception =>
              System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
          }
        case e: IOException =>
          exitCode = -1
          var content: String = e.getLocalizedMessage
          if (content != null) {
            content = content.split("\n")(0)
          }
          System.err.println(cmd.substring(1) + ": " + content)
      }
    })
    exitCode
  }

  /**
   * Move/rename file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination
   * must be a directory. Otherwise, IOException is thrown.
   * exception: IOException
   */
  private def rename(argv: Array[String], conf: Configuration): Int = {
    var i: Int = 0
    var exitCode: Int = 0
    val cmd: String = argv(i)
    i += 1
    val dest: String = argv(argv.length - 1)
    if (argv.length > 3) {
      val dst: Path = new Path(dest)
      val dstFs: FileSystem = dst.getFileSystem(getConf)
      if (!dstFs.isDirectory(dst)) {
        throw new IOException("When moving multiple files, "
          + "destination " + dest + " should be a directory.")
      }
    }
    val params = argv.slice(i, argv.length - 1)
    params.foreach(argument => {
      try {
        rename(argument, dest)
      }
      catch {
        case e: RemoteException =>
          exitCode = -1
          try {
            val content: Array[String] = e.getLocalizedMessage.split("\n")
            System.err.println(cmd.substring(1) + ": " + content(0))
          }
          catch {
            case ex: Exception =>
              System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
          }
        case e: IOException =>
          exitCode = -1
          System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage)
      }
    })
    exitCode
  }

  /**
   * Copy file(s) to a destination file. Multiple source
   * files can be specified. The destination is the last element of
   * the argvp[] array.
   * If multiple source files are specified, then the destination
   * must be a directory. Otherwise, IOException is thrown.
   * exception: IOException
   */
  private def copy(argv: Array[String], conf: Configuration): Int = {
    var exitCode: Int = -1
    var i: Int = 0
    val cmd: String = argv(i)
    i += 1
    val dest: String = argv(argv.length - 1)
    if (argv.length > 3) {
      val dst: Path = new Path(dest)
      if (!fs.isDirectory(dst)) {
        throw new IOException("When copying multiple files, " + "destination " + dest + " should be a directory.")
      }
    }

    val params = argv.slice(i, argv.length - 1)
    params.foreach(argument => {
      try {
        copy(argument, dest, conf)
      }
      catch {
        case e: RemoteException =>
          exitCode = -1
          try {
            val content: Array[String] = e.getLocalizedMessage.split("\n")
            System.err.println(cmd.substring(1) + ": " + content(0))
          }
          catch {
            case ex: Exception =>
              System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
          }
        case e: IOException =>
          exitCode = -1
          System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage)
      }
    })

    exitCode
  }

  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException
   */
  private def tail(cmd: Array[String], pos: Int) {
    val c: CommandFormat = new CommandFormat("tail", 1, 1)
    var src: String = null
    var path: Path = null
    try {
      val parameters: java.util.List[String] = c.parse(cmd, pos)
      src = parameters.get(0)
    }
    catch {
      case iae: IllegalArgumentException =>
        System.err.println("Usage: java " + classOf[SnackFSShell].getSimpleName + " -tail <src>")
        throw iae
    }
    path = new Path(src)
    val srcFs: FileSystem = path.getFileSystem(getConf)
    if (srcFs.isDirectory(path)) {
      throw new IOException("Source must be a file.")
    }
    val fileSize: Long = srcFs.getFileStatus(path).getLen
    var offset: Long = if (fileSize > 1024) fileSize - 1024 else 0

    val in: FSDataInputStream = srcFs.open(path)
    in.seek(offset)
    IOUtils.copyBytes(in, System.out, 1024, false)
    offset = in.getPos
    in.close()
  }

  /**
   * run
   */
  override def run(argv: Array[String]): Int = {
    if (argv.length < 1) {
      printUsage("")
      return -1
    }
    var exitCode: Int = -1
    var i: Int = 0
    val cmd: String = argv(i)
    i += 1
    if (("-put" == cmd) || ("-test" == cmd) || ("-copyFromLocal" == cmd) || ("-moveFromLocal" == cmd)) {
      if (argv.length < 3) {
        printUsage(cmd)
        return exitCode
      }
    }
    else if (("-get" == cmd) || ("-copyToLocal" == cmd) || ("-moveToLocal" == cmd)) {
      if (argv.length < 3) {
        printUsage(cmd)
        return exitCode
      }
    }
    else if (("-mv" == cmd) || ("-cp" == cmd)) {
      if (argv.length < 3) {
        printUsage(cmd)
        return exitCode
      }
    }
    else if (("-rm" == cmd) || ("-rmr" == cmd) || ("-cat" == cmd) || ("-mkdir" == cmd) || ("-touchz" == cmd) || ("-stat" == cmd) || ("-text" == cmd)) {
      if (argv.length < 2) {
        printUsage(cmd)
        return exitCode
      }
    }
    try {
      init()
    }
    catch {
      case v: RPC.VersionMismatch =>
        System.err.println("Version Mismatch between client and server" + "... command aborted.")
        return exitCode

      case e: IOException =>
        System.err.println("Bad connection to FS. command aborted. exception: " + e.getLocalizedMessage)
        return exitCode

    }
    exitCode = 0
    try {
      if (("-put" == cmd) || ("-copyFromLocal" == cmd)) {
        val srcs = copyReqdArgs(argv)
        copyFromLocal(srcs, argv(argv.length - 1))
      }
      else if (("-get" == cmd) || ("-copyToLocal" == cmd)) {
        copyToLocal(argv, i)
      }
      else if ("-getmerge" == cmd) {
        if (argv.length > i + 2)
          copyMergeToLocal(argv(i), new Path(argv(i + 1)), argv(i + 2).toBoolean)
        else
          copyMergeToLocal(argv(i), new Path(argv(i + 1)))
      }
      else if ("-cat" == cmd) {
        exitCode = doall(cmd, argv, i)
      }
      else if ("-text" == cmd) {
        exitCode = doall(cmd, argv, i)
      }

      //TODO -- persmission related operations
      /*else if (("-chmod" == cmd) || ("-chown" == cmd) || ("-chgrp" == cmd)) {
        exitCode = FsShellPermissions.changePermissions(fs, cmd, argv, i, this)
      }*/

      else if ("-ls" == cmd) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i)
        }
        else {
          exitCode = ls(Path.CUR_DIR, recursive = false)
        }
      }
      else if ("-lsr" == cmd) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i)
        }
        else {
          exitCode = ls(Path.CUR_DIR, recursive = true)
        }
      }
      else if ("-mv" == cmd) {
        exitCode = rename(argv, getConf)
      }
      else if ("-cp" == cmd) {
        exitCode = copy(argv, getConf)
      }
      else if ("-rm" == cmd) {
        exitCode = doall(cmd, argv, i)
      }
      else if ("-rmr" == cmd) {
        exitCode = doall(cmd, argv, i)
      }
      else if ("-du" == cmd) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i)
        }
        else {
          du(".")
        }
      }
      else if ("-dus" == cmd) {
        if (i < argv.length) {
          exitCode = doall(cmd, argv, i)
        }
        else {
          dus(".")
        }
      }
      else if (Count.matches(cmd)) {
        exitCode = new Count(argv, i, getConf).runAll
      }
      else if ("-mkdir" == cmd) {
        exitCode = doall(cmd, argv, i)
      }
      else if ("-touchz" == cmd) {
        exitCode = doall(cmd, argv, i)
      }
      else if ("-test" == cmd) {
        exitCode = test(argv, i)
      }
      else if ("-stat" == cmd) {
        if (i + 1 < argv.length) {
          stat(argv(i).toCharArray, argv(i + 1))
        } else {
          stat("%y".toCharArray, argv(i))
        }
      }
      else if ("-help" == cmd) {
        if (i < argv.length) {
          printHelp(argv(i))
        }
        else {
          printHelp("")
        }
      }
      else if ("-tail" == cmd) {
        tail(argv, i)
      }
      else {
        exitCode = -1
        System.err.println(cmd.substring(1) + ": Unknown command")
        printUsage("")
      }
    }
    catch {
      case arge: IllegalArgumentException =>
        exitCode = -1
        System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage)
        printUsage(cmd)
      case e: RemoteException =>
        exitCode = -1
        try {
          val content: Array[String] = e.getLocalizedMessage.split("\n")
          System.err.println(cmd.substring(1) + ": " + content(0))
        }
        catch {
          case ex: Exception =>
            System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
        }
      case e: IOException =>
        exitCode = -1
        System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage)
      case re: Exception =>
        exitCode = -1
        System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage)
    }
    finally {
    }
    exitCode
  }

}

object SnackFSShell {
  /**
   * main() has some simple utility methods
   */
  def main(argv: Array[String]) {
    val shell: SnackFSShell = new SnackFSShell
    var res: Int = 0
    try {
      res = ToolRunner.run(shell, argv)
    }
    finally {
      shell.close()
    }
    System.exit(res)
  }
}