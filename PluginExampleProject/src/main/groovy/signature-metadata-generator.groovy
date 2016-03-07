import com.alpine.plugin.core.OperatorSignature
import groovy.xml.MarkupBuilder
import org.reflections.Reflections

import java.nio.file.Path
import java.nio.file.Paths

/**
 * Run "mvn compile gplus:execute" from the command line to execute this. It generates the plugins.xml file.
 */
def reflections = new Reflections("").getSubTypesOf(OperatorSignature)
Path baseDir = Paths.get(".").toAbsolutePath().normalize()
def resourcesDir = baseDir.resolve("src").resolve("main").resolve("resources")
pluginsXML = new File(resourcesDir.toFile(), "plugins.xml")
def sw = new FileWriter(pluginsXML)
def xml = new MarkupBuilder(sw)
xml.mkp.xmlDeclaration(version: "1.0", encoding: "UTF-8")
xml."alpine-plugins" {
    reflections.each { r ->
        plugin() {
            "signature-class"(r.getCanonicalName())
        }
    }
}