package org.ektorp.spring;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.ektorp.CouchDbConnector;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.ektorp.impl.StreamedCouchDbConnector;
import org.ektorp.support.CouchDbDocument;
import org.ektorp.support.CouchDbRepositorySupport;
import org.ektorp.support.View;
import org.ektorp.support.Views;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class Issue222Test {

    private final static Logger log = LoggerFactory.getLogger(Issue222Test.class);

    @Views({
            @View(name = "some", map = "function(doc) { if (doc.type == 'Contact' ) emit( null, doc._id )}")
    })
    public static class Contact extends CouchDbDocument {

        public String type = Contact.class.getSimpleName();
        public String firstName;
        public String lastName;
        public int age;

        public Contact() {

        }

        public Contact(String firstName, String lastName, int age) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

    }

    @Rule
    public TestName testName = new TestName();

    private StdCouchDbInstance couchDbInstance;

    private StdCouchDbConnector db;

    @Autowired
    private HttpClient ektorpHttpClient;

    @Before
    public void before() throws Exception {
        // this is not working, it uses the Apache HC 3.x old style builder
        //couchDbInstance = createStdCouchDbInstance();

        // this is not working, it uses the Apache HC 3.x old style builder, with just some more configuration
        /*
        ExtendedHttpClientFactoryBean extendedHttpClientFactoryBean = new ExtendedHttpClientFactoryBean();
        Properties properties = new Properties();
        properties.setProperty("host", "localhost");
        properties.setProperty("port", "5984");
        extendedHttpClientFactoryBean.setProperties(properties);
        extendedHttpClientFactoryBean.afterPropertiesSet();
        HttpClient httpClient = extendedHttpClientFactoryBean.getObject();
        couchDbInstance = new StdCouchDbInstance(httpClient);
        */

        couchDbInstance = new StdCouchDbInstance(ektorpHttpClient);

        String databaseName = getClass().getSimpleName() + "-" + testName.getMethodName() + "-jediDB";
        if (couchDbInstance.checkIfDbExists(databaseName)) {
            couchDbInstance.deleteDatabase(databaseName);
        }
        db = new StreamedCouchDbConnector(databaseName, couchDbInstance);
        db.createDatabaseIfNotExists();
    }

    @Test
    public void shouldCreateRetrieveUpdateDeleteDocumentProperly() {
        String documentId = "luke";

        // search for the document in the empty database ...
        Contact contact = db.find(Contact.class, documentId);
        assertNull(contact);
        log.info("Document with id '" + documentId + "' was not found in the Database, will create it ...");

        // create the document and save in to the database ...
        contact = new Contact("Luke", "Skywalker", 19);
        contact.setId(documentId);
        db.create(contact);
        assertNotNull(contact.getRevision());
        log.info("Created document with id '" + documentId + "' and revision '" + contact.getRevision());
        log.info("contact = " + ToStringBuilder.reflectionToString(contact));

        // update the document multiple times ...
        for (int i = 0; i < 10; i++) {
            contact = db.find(Contact.class, documentId);
            assertNotNull(contact);
            log.info("Document with id '" + documentId + "' was found in the Database, will update it ...");
            String previousRevision = contact.getRevision();
            assertNotNull(previousRevision);
            db.update(contact);
            String newRevision = contact.getRevision();
            assertNotSame(previousRevision, newRevision);
            log.info("Updated document with id '" + documentId + "' and revision '" + contact.getRevision());
            log.info("contact = " + ToStringBuilder.reflectionToString(contact));
        }

        // delete the document ...
        String rev = db.delete(contact);
        log.info("rev = " + rev);
    }

    static class ContactRepository extends CouchDbRepositorySupport<Contact> {

        protected ContactRepository(CouchDbConnector db) {
            super(Contact.class, db);
            initStandardDesignDocument();
        }

    }

    @Test
    public void shouldWorkUsingCouchDbRepositorySupportAndLoadingAsStreamBefore() throws IOException {
        ContactRepository contactRepository = new ContactRepository(db);
        contactRepository.add(new Contact("Anakin", "Skywalker", 45));

        InputStream designDocumentStream = null;
        try {
            designDocumentStream = db.getAsStream("_design/" + Contact.class.getSimpleName());
            String designDocumentAsString = IOUtils.toString(designDocumentStream, "UTF-8");
            log.info("designDocumentAsString = " + designDocumentAsString);
        } finally {
            IOUtils.closeQuietly(designDocumentStream);
        }

        List<Contact> allContacts = contactRepository.getAll();
        log.info("allContacts = " + ToStringBuilder.reflectionToString(allContacts));
        assertEquals(1, allContacts.size());
    }

    @Test
    public void shouldWorkUsingCouchDbRepositorySupportAndSleepingBefore() throws IOException, InterruptedException {
        ContactRepository contactRepository = new ContactRepository(db);
        contactRepository.add(new Contact("Anakin", "Skywalker", 45));

        Thread.sleep(3 * 1000l);

        List<Contact> allContacts = contactRepository.getAll();
        log.info("allContacts = " + ToStringBuilder.reflectionToString(allContacts));
        assertEquals(1, allContacts.size());
    }

    @Test
    public void shouldWorkUsingCouchDbRepositorySupportTwice() throws IOException, InterruptedException {
        ContactRepository contactRepository = new ContactRepository(db);
        contactRepository.add(new Contact("Anakin", "Skywalker", 45));

        List<Contact> allContacts = null;
        for (int i = 0; i < 4; i++) {
            try {
                allContacts = contactRepository.getAll();
                log.info("allContacts = " + ToStringBuilder.reflectionToString(allContacts));
                assertEquals(1, allContacts.size());
            } catch (NullPointerException e) {
                log.error("NullPointerException occurred", e);
            }
        }

        InputStream designDocumentStream = null;
        try {
            designDocumentStream = db.getAsStream("_design/" + Contact.class.getSimpleName());
            String designDocumentAsString = IOUtils.toString(designDocumentStream, "UTF-8");
            log.info("designDocumentAsString = " + designDocumentAsString);
        } finally {
            IOUtils.closeQuietly(designDocumentStream);
        }

        allContacts = contactRepository.getAll();
        log.info("allContacts = " + ToStringBuilder.reflectionToString(allContacts));
        assertNotNull(allContacts);
        assertEquals(1, allContacts.size());
    }

    @Test
    public void shouldWorkUsingCouchDbRepositorySupport() throws IOException, InterruptedException {
        ContactRepository contactRepository = new ContactRepository(db);
        contactRepository.add(new Contact("Anakin", "Skywalker", 45));

        List<Contact> allContacts = contactRepository.getAll();
        log.info("allContacts = " + ToStringBuilder.reflectionToString(allContacts));
        assertEquals(1, allContacts.size());
    }


    private StdCouchDbInstance createStdCouchDbInstance() {
        StdHttpClient.Builder builder = new StdHttpClient.Builder()
                .host("localhost")
                .port(5984);

        Properties props = new Properties();
        File propFile = new File(System.getProperty("user.home"), "couchDbCredentials.properties");
        InputStream is = null;
        try {
            is = new FileInputStream(propFile);
            props.load(is);
        } catch (IOException ex) {
            props = null;
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }

        if (props != null) {
            builder.username(props.getProperty("couch.user")).password(props.getProperty("couch.password"));
        }

        return new StdCouchDbInstance(builder.build());
    }

}
