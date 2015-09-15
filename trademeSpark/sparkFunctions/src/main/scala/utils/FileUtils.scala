package utils

import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption;
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs._

object FileUtils {
	def delete(file: File) {
	  if (file.isDirectory) 
	    Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
	  file.delete
	}

	def mergeMultiParts(srcPath: Path, dstPath: Path): Unit =  {
		Files.deleteIfExists(dstPath)
		
		if (Files.isDirectory(srcPath)) {
			srcPath.toFile.listFiles.map(file => {
				val filePath = file.toPath
				if (!Files.isDirectory(filePath) && (Files.size(filePath)>0) && !file.getName.endsWith("crc")) {
					val bytes = Files.readAllBytes(filePath)
					Files.write(dstPath, Files.readAllBytes(filePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
				}
			})
		}
	}

	def mergeMultiParts(srcPathAsStr: String, dstPathAsStr: String): Unit =  {
		val srcPath = Paths.get(srcPathAsStr)
		val dstPath = Paths.get(dstPathAsStr)
		mergeMultiParts(srcPath, dstPath)
	}
}