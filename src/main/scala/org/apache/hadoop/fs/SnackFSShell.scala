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

  private val SETREP_SHORT_USAGE: String = "-setrep [-R] [-w] <rep> <path/file>"
  private val GET_SHORT_USAGE: String = "-get [-ignoreCrc] [-crc] <src> <localdst>"
  private val COPYTOLOCAL_SHORT_USAGE: String = GET_SHORT_USAGE.replace("-get", "-copyToLocal")
  private val TAIL_USAGE: String = "-tail [-f] <file>"

  val dateForm: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm")

  private val trash: Trash = null

  private def printUsage(cmd: String) {
    val unsupported: String = "This command is not supported"
    val prefix: String = "Usage: java " + classOf[SnackFSShell].getSimpleName
    if ("-fs" == cmd) {
      System.err.println(prefix + " [-fs <local | file system URI>]")
    }
    else if ("-conf" == cmd) {
      //      System.err.println("Usage: java FsShell" + " [-conf <configuration file>]")
      System.err.println(unsupported)
    }
    else if ("-D" == cmd) {
      //      System.err.println("Usage: java FsShell" + " [-D <[property=value>]")
      System.err.println(unsupported)
    }
    else if (("-ls" == cmd) || ("-lsr" == cmd) || ("-du" == cmd) || ("-dus" == cmd) || ("-touchz" == cmd) || ("-mkdir" == cmd) || ("-text" == cmd)) {
      System.err.println(prefix + " [" + cmd + " <path>]")
    }
    else if ("-df" == cmd) {
      System.err.println(prefix + " [" + cmd + " [<path>]]")
    }
    else if (Count.matches(cmd)) {
      System.err.println(prefix + " [" + Count.USAGE + "]")
    }
    else if (("-rm" == cmd) || ("-rmr" == cmd)) {
      System.err.println(prefix + " [" + cmd + " [-skipTrash] <src>]")
    }
    else if (("-mv" == cmd) || ("-cp" == cmd)) {
      System.err.println(prefix + " [" + cmd + " <src> <dst>]")
    }
    else if (("-put" == cmd) || ("-copyFromLocal" == cmd) || ("-moveFromLocal" == cmd)) {
      System.err.println(prefix + " [" + cmd + " <localsrc> ... <dst>]")
    }
    else if ("-get" == cmd) {
      System.err.println(prefix + " [" + GET_SHORT_USAGE + "]")
    }
    else if ("-copyToLocal" == cmd) {
      System.err.println(prefix + " [" + COPYTOLOCAL_SHORT_USAGE + "]")
    }
    else if ("-moveToLocal" == cmd) {
      System.err.println(prefix + " [" + cmd + " [-crc] <src> <localdst>]")
    }
    else if ("-cat" == cmd) {
      System.err.println(prefix + " [" + cmd + " <src>]")
    }
    else if ("-setrep" == cmd) {
      //      System.err.println(prefix+" [" + SETREP_SHORT_USAGE + "]")
      System.err.println(unsupported)
    }
    else if ("-test" == cmd) {
      System.err.println(prefix + " [-test -[ezd] <path>]")
    }
    else if ("-stat" == cmd) {
      System.err.println(prefix + " [-stat [format] <path>]")
    }
    else if ("-tail" == cmd) {
      System.err.println(prefix + " [" + TAIL_USAGE + "]")
    }
    else {
      System.err.println(prefix)
      System.err.println("           [-ls <path>]")
      System.err.println("           [-lsr <path>]")
      System.err.println("           [-df [<path>]]")
      System.err.println("           [-du <path>]")
      System.err.println("           [-dus <path>]")
      System.err.println("           [" + Count.USAGE + "]")
      System.err.println("           [-mv <src> <dst>]")
      System.err.println("           [-cp <src> <dst>]")
      System.err.println("           [-rm [-skipTrash] <path>]")
      System.err.println("           [-rmr [-skipTrash] <path>]")
      System.err.println("           [-expunge]")
      System.err.println("           [-put <localsrc> ... <dst>]")
      System.err.println("           [-copyFromLocal <localsrc> ... <dst>]")
      System.err.println("           [-moveFromLocal <localsrc> ... <dst>]")
      System.err.println("           [" + GET_SHORT_USAGE + "]")
      System.err.println("           [-getmerge <src> <localdst> [addnl]]")
      System.err.println("           [-cat <src>]")
      System.err.println("           [-text <src>]")
      System.err.println("           [" + COPYTOLOCAL_SHORT_USAGE + "]")
      System.err.println("           [-moveToLocal [-crc] <src> <localdst>]")
      System.err.println("           [-mkdir <path>]")
      //      System.err.println("           [" + SETREP_SHORT_USAGE + "]")
      System.err.println("           [-touchz <path>]")
      System.err.println("           [-test -[ezd] <path>]")
      System.err.println("           [-stat [format] <path>]")
      System.err.println("           [" + TAIL_USAGE + "]")
      /* System.err.println("           [" + FsShellPermissions.CHMOD_USAGE + "]")
       System.err.println("           [" + FsShellPermissions.CHOWN_USAGE + "]")
       System.err.println("           [" + FsShellPermissions.CHGRP_USAGE + "]")*/
      System.err.println("           [-help [cmd]]")
      System.err.println()
      //      ToolRunner.printGenericCommandUsage(System.err)
    }
  }

  private def printHelp(cmd: String) {
    val unsupported: String = "This command is not supported"

    val summary: String = "snackfs fs is the command to execute fs commands. " +
      "The full syntax is: \n\n" + "snackfs fs [-fs <local | file system URI>]" +
      //      " [-conf <configuration file>]\n\t" + "[-D <property=value>]" +
      " \n\t[-ls <path>] [-lsr <path>] [-du <path>]\n\t" +
      "[-dus <path>] [-mv <src> <dst>] [-cp <src> <dst>] [-rm [-skipTrash] <src>]\n\t" +
      "[-rmr [-skipTrash] <src>] [-put <localsrc> ... <dst>] [-copyFromLocal <localsrc> ... <dst>]\n\t" +
      "[-moveFromLocal <localsrc> ... <dst>] [" + GET_SHORT_USAGE + "\n\t" +
      "[-getmerge <src> <localdst> [addnl]] [-cat <src>]\n\t" +
      "[" + COPYTOLOCAL_SHORT_USAGE + "] [-moveToLocal <src> <localdst>]\n\t" +
      "[-mkdir <path>] [-report] [" + SETREP_SHORT_USAGE + "]\n\t" +
      "[-touchz <path>] [-test -[ezd] <path>] [-stat [format] <path>]\n\t" +
      "[-tail [-f] <path>] [-text <path>]\n\t" +
      //"[" + FsShellPermissions.CHMOD_USAGE + "]\n\t" + "[" + FsShellPermissions.CHOWN_USAGE + "]\n\t" + "[" + FsShellPermissions.CHGRP_USAGE + "]\n\t" +
      "[" + Count.USAGE + "]\n\t" + "[-help [cmd]]\n"

    /*val conf: String = "-conf <configuration file>:  Specify an application configuration file."
    val D: String = "-D <property=value>:  Use value for given property."*/
    val fs: String = "-fs [local | <file system URI>]: \tSpecify the file system to use.\n" + "\t\tIf not specified, the current configuration is used, \n" + "\t\ttaken from the following, in increasing precedence: \n" + "\t\t\tcore-default.xml inside the hadoop jar file \n" + "\t\t\tcore-site.xml in $HADOOP_CONF_DIR \n" + "\t\t'local' means use the local file system as your DFS. \n" + "\t\t<file system URI> specifies a particular file system to \n" + "\t\tcontact. This argument is optional but if used must appear\n" + "\t\tappear first on the command line.  Exactly one additional\n" + "\t\targument must be specified. \n"
    val ls: String = "-ls <path>: \tList the contents that match the specified file pattern. If\n" + "\t\tpath is not specified, the contents of /user/<currentUser>\n" + "\t\twill be listed. Directory entries are of the form \n" + "\t\t\tdirName (full path) <dir> \n" + "\t\tand file entries are of the form \n" + "\t\t\tfileName(full path) <r n> size \n" + "\t\twhere n is the number of replicas specified for the file \n" + "\t\tand size is the size of the file, in bytes.\n"
    val lsr: String = "-lsr <path>: \tRecursively list the contents that match the specified\n" + "\t\tfile pattern.  Behaves very similarly to snackfs fs -ls,\n" + "\t\texcept that the data is shown for all the entries in the\n" + "\t\tsubtree.\n"
    val du: String = "-du <path>: \tShow the amount of space, in bytes, used by the files that \n" + "\t\tmatch the specified file pattern.  Equivalent to the unix\n" + "\t\tcommand \"du -sb <path>/*\" in case of a directory, \n" + "\t\tand to \"du -b <path>\" in case of a file.\n" + "\t\tThe output is in the form \n" + "\t\t\tname(full path) size (in bytes)\n"
    val dus: String = "-dus <path>: \tShow the amount of space, in bytes, used by the files that \n" + "\t\tmatch the specified file pattern.  Equivalent to the unix\n" + "\t\tcommand \"du -sb\"  The output is in the form \n" + "\t\t\tname(full path) size (in bytes)\n"
    val mv: String = "-mv <src> <dst>:   Move files that match the specified file pattern <src>\n" + "\t\tto a destination <dst>.  When moving multiple files, the \n" + "\t\tdestination must be a directory. \n"
    val cp: String = "-cp <src> <dst>:   Copy files that match the file pattern <src> to a \n" + "\t\tdestination.  When copying multiple files, the destination\n" + "\t\tmust be a directory. \n"
    val rm: String = "-rm [-skipTrash] <src>: \tDelete all files that match the specified file pattern.\n" + "\t\tEquivalent to the Unix command \"rm <src>\"\n" + "\t\t-skipTrash option bypasses trash, if enabled, and immediately\n" + "deletes <src>"
    val rmr: String = "-rmr [-skipTrash] <src>: \tRemove all directories which match the specified file \n" + "\t\tpattern. Equivalent to the Unix command \"rm -rf <src>\"\n" + "\t\t-skipTrash option bypasses trash, if enabled, and immediately\n" + "deletes <src>"
    val put: String = "-put <localsrc> ... <dst>: \tCopy files " + "from the local file system \n\t\tinto fs. \n"
    val copyFromLocal: String = "-copyFromLocal <localsrc> ... <dst>:" + " Identical to the -put command.\n"
    val moveFromLocal: String = "-moveFromLocal <localsrc> ... <dst>:" + " Same as -put, except that the source is\n\t\tdeleted after it's copied.\n"
    val get: String = GET_SHORT_USAGE + ":  Copy files that match the file pattern <src> \n" + "\t\tto the local name.  <src> is kept.  When copying mutiple, \n" + "\t\tfiles, the destination must be a directory. \n"
    val getmerge: String = "-getmerge <src> <localdst>:  Get all the files in the directories that \n" + "\t\tmatch the source file pattern and merge and sort them to only\n" + "\t\tone file on local fs. <src> is kept.\n"
    val cat: String = "-cat <src>: \tFetch all files that match the file pattern <src> \n" + "\t\tand display their content on stdout.\n"
    val text: String = "-text <src>: \tTakes a source file and outputs the file in text format.\n" + "\t\tThe allowed formats are zip and TextRecordInputStream.\n"
    val copyToLocal: String = COPYTOLOCAL_SHORT_USAGE + ":  Identical to the -get command.\n"
    val moveToLocal: String = "-moveToLocal <src> <localdst>:  Not implemented yet \n"
    val mkdir: String = "-mkdir <path>: \tCreate a directory in specified location. \n"
    //    val setrep: String = SETREP_SHORT_USAGE + ":  Set the replication level of a file. \n" + "\t\tThe -R flag requests a recursive change of replication level \n" + "\t\tfor an entire tree.\n"
    val touchz: String = "-touchz <path>: Write a timestamp in yyyy-MM-dd HH:mm:ss format\n" + "\t\tin a file at <path>. An error is returned if the file exists with non-zero length\n"
    val test: String = "-test -[ezd] <path>: If file { exists, has zero length, is a directory\n" + "\t\tthen return 0, else return 1.\n"
    val stat: String = "-stat [format] <path>: Print statistics about the file/directory at <path>\n" + "\t\tin the specified format. Format accepts filesize in blocks (%b), filename (%n),\n" + "\t\tblock size (%o), replication (%r), modification date (%y, %Y)\n"
    val tail: String = TAIL_USAGE + ":  Show the last 1KB of the file. \n" + "\t\tThe -f option shows apended data as the file grows. \n"
    /*val chmod: String = FsShellPermissions.CHMOD_USAGE + "\n" + "\t\tChanges permissions of a file.\n" + "\t\tThis works similar to shell's chmod with a few exceptions.\n\n" + "\t-R\tmodifies the files recursively. This is the only option\n" + "\t\tcurrently supported.\n\n" + "\tMODE\tMode is same as mode used for chmod shell command.\n" + "\t\tOnly letters recognized are 'rwxX'. E.g. a+r,g-w,+rwx,o=r\n\n" + "\tOCTALMODE Mode specifed in 3 digits. Unlike shell command,\n" + "\t\tthis requires all three digits.\n" + "\t\tE.g. 754 is same as u=rwx,g=rx,o=r\n\n" + "\t\tIf none of 'augo' is specified, 'a' is assumed and unlike\n" + "\t\tshell command, no umask is applied.\n"
    val chown: String = FsShellPermissions.CHOWN_USAGE + "\n" + "\t\tChanges owner and group of a file.\n" + "\t\tThis is similar to shell's chown with a few exceptions.\n\n" + "\t-R\tmodifies the files recursively. This is the only option\n" + "\t\tcurrently supported.\n\n" + "\t\tIf only owner or group is specified then only owner or\n" + "\t\tgroup is modified.\n\n" + "\t\tThe owner and group names may only cosists of digits, alphabet,\n" + "\t\tand any of '-_.@/' i.e. [-_.@/a-zA-Z0-9]. The names are case\n" + "\t\tsensitive.\n\n" + "\t\tWARNING: Avoid using '.' to separate user name and group though\n" + "\t\tLinux allows it. If user names have dots in them and you are\n" + "\t\tusing local file system, you might see surprising results since\n" + "\t\tshell command 'chown' is used for local files.\n"
    val chgrp: String = FsShellPermissions.CHGRP_USAGE + "\n" + "\t\tThis is equivalent to -chown ... :GROUP ...\n"*/
    val help: String = "-help [cmd]: \tDisplays help for given command or all commands if none\n" + "\t\tis specified.\n"
    if ("fs" == cmd) {
      System.out.println(fs)
    }
    else if ("conf" == cmd) {
      //      System.out.println(conf)
      System.out.println(unsupported)
    }
    else if ("D" == cmd) {
      //      System.out.println(D)
      System.out.println(unsupported)
    }
    else if ("ls" == cmd) {
      System.out.println(ls)
    }
    else if ("lsr" == cmd) {
      System.out.println(lsr)
    }
    else if ("du" == cmd) {
      System.out.println(du)
    }
    else if ("dus" == cmd) {
      System.out.println(dus)
    }
    else if ("rm" == cmd) {
      System.out.println(rm)
    }
    else if ("rmr" == cmd) {
      System.out.println(rmr)
    }
    else if ("mkdir" == cmd) {
      System.out.println(mkdir)
    }
    else if ("mv" == cmd) {
      System.out.println(mv)
    }
    else if ("cp" == cmd) {
      System.out.println(cp)
    }
    else if ("put" == cmd) {
      System.out.println(put)
    }
    else if ("copyFromLocal" == cmd) {
      System.out.println(copyFromLocal)
    }
    else if ("moveFromLocal" == cmd) {
      System.out.println(moveFromLocal)
    }
    else if ("get" == cmd) {
      System.out.println(get)
    }
    else if ("getmerge" == cmd) {
      System.out.println(getmerge)
    }
    else if ("copyToLocal" == cmd) {
      System.out.println(copyToLocal)
    }
    else if ("moveToLocal" == cmd) {
      System.out.println(moveToLocal)
    }
    else if ("cat" == cmd) {
      System.out.println(cat)
    }
    else if ("get" == cmd) {
      System.out.println(get)
    }
    else if ("setrep" == cmd) {
      //      System.out.println(setrep)
      System.out.println(unsupported)
    }
    else if ("touchz" == cmd) {
      System.out.println(touchz)
    }
    else if ("test" == cmd) {
      System.out.println(test)
    }
    else if ("text" == cmd) {
      System.out.println(text)
    }
    else if ("stat" == cmd) {
      System.out.println(stat)
    }
    else if ("tail" == cmd) {
      System.out.println(tail)
    }
    else if ("chmod" == cmd) {
      //       System.out.println(chmod)
      System.out.println(unsupported)
    }
    else if ("chown" == cmd) {
      //       System.out.println(chown)
      System.out.println(unsupported)
    }
    else if ("chgrp" == cmd) {
      //       System.out.println(chgrp)
      System.out.println(unsupported)
    }
    else if (Count.matches(cmd)) {
      System.out.println(Count.DESCRIPTION)
    }
    else if ("help" == cmd) {
      System.out.println(help)
    }
    else {
      System.out.println(summary)
      System.out.println(fs)
      System.out.println(ls)
      System.out.println(lsr)
      System.out.println(du)
      System.out.println(dus)
      System.out.println(mv)
      System.out.println(cp)
      System.out.println(rm)
      System.out.println(rmr)
      System.out.println(put)
      System.out.println(copyFromLocal)
      System.out.println(moveFromLocal)
      System.out.println(get)
      System.out.println(getmerge)
      System.out.println(cat)
      System.out.println(copyToLocal)
      System.out.println(moveToLocal)
      System.out.println(mkdir)
      //      System.out.println(setrep)
      System.out.println(tail)
      System.out.println(touchz)
      System.out.println(test)
      System.out.println(text)
      System.out.println(stat)
      /* System.out.println(chmod)
       System.out.println(chown)
       System.out.println(chgrp)*/
      System.out.println(Count.DESCRIPTION)
      System.out.println(help)
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
        case e: IOException => {
          System.err.println(cmd + ": could not get get listing for '" + path + "' : " + e.getMessage.split("\n")(0))
          null
        }
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
    var rmSkipTrash: Boolean = false
    if ((("-rm" == cmd) || ("-rmr" == cmd)) && ("-skipTrash" == argv(i))) {
      rmSkipTrash = true
      i += 1
    }

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
        case e: RemoteException => {
          exitCode = -1
          try {
            val content: Array[String] = e.getLocalizedMessage.split("\n")
            System.err.println(cmd.substring(1) + ": " + content(0))
          }
          catch {
            case ex: Exception => {
              System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
            }
          }
        }
        case e: IOException => {
          exitCode = -1
          var content: String = e.getLocalizedMessage
          if (content != null) {
            content = content.split("\n")(0)
          }
          System.err.println(cmd.substring(1) + ": " + content)
        }
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
   * @exception: IOException
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
        case e: RemoteException => {
          exitCode = -1
          try {
            val content: Array[String] = e.getLocalizedMessage.split("\n")
            System.err.println(cmd.substring(1) + ": " + content(0))
          }
          catch {
            case ex: Exception => {
              System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
            }
          }
        }
        case e: IOException => {
          exitCode = -1
          System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage)
        }
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
   * @exception: IOException
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
        case e: RemoteException => {
          exitCode = -1
          try {
            val content: Array[String] = e.getLocalizedMessage.split("\n")
            System.err.println(cmd.substring(1) + ": " + content(0))
          }
          catch {
            case ex: Exception => {
              System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
            }
          }
        }
        case e: IOException => {
          exitCode = -1
          System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage)
        }
      }
    })

    exitCode
  }

  private def expunge() {
    trash.expunge()
    trash.checkpoint()
  }

  /**
   * Parse the incoming command string
   * @param cmd
   * @param pos ignore anything before this pos in cmd
   * @throws IOException
   */
  private def tail(cmd: Array[String], pos: Int) {
    val c: CommandFormat = new CommandFormat("tail", 1, 1, "f")
    var src: String = null
    var path: Path = null
    try {
      val parameters: java.util.List[String] = c.parse(cmd, pos)
      src = parameters.get(0)
    }
    catch {
      case iae: IllegalArgumentException => {
        System.err.println("Usage: java FsShell " + TAIL_USAGE)
        throw iae
      }
    }
    val foption: Boolean = c.getOpt("f")
    path = new Path(src)
    val srcFs: FileSystem = path.getFileSystem(getConf)
    if (srcFs.isDirectory(path)) {
      throw new IOException("Source must be a file.")
    }
    var fileSize: Long = srcFs.getFileStatus(path).getLen
    var offset: Long = if (fileSize > 1024) fileSize - 1024 else 0

    //TODO didnt understand the part where `true` is passed in while loop
    while (true) {
      val in: FSDataInputStream = srcFs.open(path)
      in.seek(offset)
      IOUtils.copyBytes(in, System.out, 1024, false)
      offset = in.getPos
      in.close()
      if (foption) {
        fileSize = srcFs.getFileStatus(path).getLen
        offset = if (fileSize > offset) offset else fileSize

        //TODO didnt understand why this is done
        try {
          Thread.sleep(5000)
        }
        catch {
          case e: InterruptedException => {
            throw e
          }
        }
      }
    }
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
      case v: RPC.VersionMismatch => {
        System.err.println("Version Mismatch between client and server" + "... command aborted.")
        return exitCode
      }
      case e: IOException => {
        System.err.println("Bad connection to FS. command aborted. exception: " + e.getLocalizedMessage)
        return exitCode
      }
    }
    exitCode = 0
    try {
      if (("-put" == cmd) || ("-copyFromLocal" == cmd)) {
        val srcs = copyReqdArgs(argv)
        copyFromLocal(srcs, argv(argv.length - 1))
      }
      else if ("-moveFromLocal" == cmd) {
        val srcs = copyReqdArgs(argv)
        moveFromLocal(srcs, argv(argv.length - 1))
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
      else if ("-moveToLocal" == cmd) {
        moveToLocal(argv(i), new Path(argv(i + 1)))
      }

      /* Not possible -- in Cassandra replication factor can be set only for a keyspace not row level
      else if ("-setrep" == cmd) {
         setReplication(argv, i)
       }*/

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
      else if ("-expunge" == cmd) {
        expunge()
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
      case arge: IllegalArgumentException => {
        exitCode = -1
        System.err.println(cmd.substring(1) + ": " + arge.getLocalizedMessage)
        printUsage(cmd)
      }
      case e: RemoteException => {
        exitCode = -1
        try {
          val content: Array[String] = e.getLocalizedMessage.split("\n")
          System.err.println(cmd.substring(1) + ": " + content(0))
        }
        catch {
          case ex: Exception => {
            System.err.println(cmd.substring(1) + ": " + ex.getLocalizedMessage)
          }
        }
      }
      case e: IOException => {
        exitCode = -1
        System.err.println(cmd.substring(1) + ": " + e.getLocalizedMessage)
      }
      case re: Exception => {
        exitCode = -1
        System.err.println(cmd.substring(1) + ": " + re.getLocalizedMessage)
      }
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