package utils
/**
 * LogHelper is a trait you can mix in to provide easy log4j logging 
 * for your scala classes. 
 **/
import org.apache.log4j.Logger;

 
trait LogHelper {
    val loggerName = this.getClass.getName
    lazy val logger = Logger.getLogger(loggerName)
}