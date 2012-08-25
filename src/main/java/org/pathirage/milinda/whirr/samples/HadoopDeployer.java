package org.pathirage.milinda.whirr.samples;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import static org.apache.whirr.ClusterSpec.Property.*;
import static org.apache.whirr.ClusterSpec.Property.INSTANCE_TEMPLATES;
import static org.apache.whirr.ClusterSpec.Property.PRIVATE_KEY_FILE;

public class HadoopDeployer{
    private static Logger logger = Logger.getLogger(HadoopDeployer.class.getName());

    private ClusterSpec hadoopClusterSpec;

    public void launch(File whirrConfiguration) throws ConfigurationException, IOException, InterruptedException, TransformerException, ParserConfigurationException {
        ClusterSpec hadoopClusterSpec = createClusterSpec(whirrConfiguration);
        ClusterController hadoopClusterController =
                createClusterController(hadoopClusterSpec.getServiceName());
        Cluster hadoopCluster = hadoopClusterController.launchCluster(hadoopClusterSpec);

        logger.info(String.format("Started hadoop cluster of %s instances.", hadoopCluster.getInstances().size()));

        File siteXml = File.createTempFile("hadoop-site", ".xml");
        hadoopClusterPropertiesToFile(hadoopCluster.getConfiguration(), siteXml);


        logger.info(String.format("Hadoop configuration XML can be found at %s.", siteXml.getAbsolutePath()));
        logger.info("Configuration file:");
        logger.info(FileUtils.readFileToString(siteXml));
    }

    public void destroy() throws IOException, InterruptedException {
        logger.info("Destroying hadoop cluster...");
        ClusterController hadoopClusterController =
                createClusterController(hadoopClusterSpec.getServiceName());
        hadoopClusterController.destroyCluster(hadoopClusterSpec);
        logger.info("Hadoop cluster destroyed.");
    }

    private ClusterController createClusterController(String serviceName){
        ClusterControllerFactory factory = new ClusterControllerFactory();
        ClusterController controller = factory.create(serviceName);

        if(controller == null){
            logger.warning(String.format("Unable to find the service %s, using default.", serviceName));
            controller = factory.create(null);
        }

        return controller;
    }
    private ClusterSpec createClusterSpec(File whirrConfiuration) throws ConfigurationException {
        CompositeConfiguration compositeConfiguration = new CompositeConfiguration();
        Configuration configuration = new PropertiesConfiguration(whirrConfiuration);
        compositeConfiguration.addConfiguration(configuration);

        ClusterSpec hadoopClusterSpec = new ClusterSpec(compositeConfiguration);

        for (ClusterSpec.Property required : EnumSet.of(CLUSTER_NAME, PROVIDER, IDENTITY, CREDENTIAL,
                INSTANCE_TEMPLATES, PRIVATE_KEY_FILE)) {
            if (hadoopClusterSpec.getConfiguration().getString(required.getConfigName()) == null) {
                throw new IllegalArgumentException(String.format("Option '%s' not set.",
                        required.getSimpleName()));
            }
        }

        return hadoopClusterSpec;
    }

    private void hadoopClusterPropertiesToFile(Properties props, File hadoopSiteXml) throws ParserConfigurationException, TransformerException {
        DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder documentBuilder = domFactory.newDocumentBuilder();

        Document hadoopSiteXmlDoc = documentBuilder.newDocument();

        hadoopSiteXmlDoc.setXmlVersion("1.0");
        hadoopSiteXmlDoc.setXmlStandalone(true);
        hadoopSiteXmlDoc.createProcessingInstruction("xml-stylesheet", "type=\"text/xsl\" href=\"configuration.xsl\"");

        Element configEle = hadoopSiteXmlDoc.createElement("configuration");

        hadoopSiteXmlDoc.appendChild(configEle);

        for(Map.Entry<Object, Object> entry : props.entrySet()){
            addPropertyToConfiguration(entry, configEle, hadoopSiteXmlDoc);
        }

        saveDomToFile(hadoopSiteXmlDoc, hadoopSiteXml);
    }

    private void saveDomToFile(Document dom, File destFile) throws TransformerException {
        Source source = new DOMSource(dom);

        Result result = new StreamResult(destFile);

        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.transform(source, result);
    }

    private void addPropertyToConfiguration(Map.Entry<Object, Object> entry, Element configElement, Document doc){
        Element property = doc.createElement("property");
        configElement.appendChild(property);

        Element nameEle = doc.createElement("name");
        nameEle.setTextContent(entry.getKey().toString());
        property.appendChild(nameEle);

        Element valueEle = doc.createElement("value");
        valueEle.setTextContent(entry.getValue().toString());
        property.appendChild(valueEle);
    }

    public static void main(String[] args) throws TransformerException, IOException, ConfigurationException, InterruptedException, ParserConfigurationException {
        // Whirr requires unprotected private key file.
        // ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa_whirr can be used generate private key file without password.
        // Put that into whirr-hadoop.properties file.

        HadoopDeployer hadoopDeployer = new HadoopDeployer();

        File whirrPorperties = new File("/Users/milinda/Workspace/Personal/Coding/github/whirr-sample/src/main/resources/whirr-hadoop.properties");
        hadoopDeployer.launch(whirrPorperties);
        hadoopDeployer.destroy();
        System.exit(0);
    }
}
