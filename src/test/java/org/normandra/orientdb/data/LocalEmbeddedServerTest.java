package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class LocalEmbeddedServerTest {

    public LocalEmbeddedServerTest() {
        System.setProperty("ORIENTDB_ROOT_PASSWORD", "admin");
    }

    File orientDir = new File("src/test/resources/orientdb-sample");

    String serverUser = "root";

    String serverPwd = "admin";

    @Test
    public void testConfiguration() throws Exception {
        LocalEmbeddedServer server = new LocalEmbeddedServer(orientDir, serverUser, serverPwd);
        int port = server.getHttpPort();
        Assert.assertTrue(port > 0);
    }

    @Test
    public void testStartStop() throws Exception {
        LocalEmbeddedServer server = new LocalEmbeddedServer(orientDir, serverUser, serverPwd);
        server.startIfNotRunning();
        Assert.assertTrue(server.waitUntilReady(30 * 1000));
        Assert.assertTrue(server.isRunning());
        server.stop();
        Assert.assertFalse(server.isRunning());
    }

    @Test
    public void testDatabasePool() throws Exception {
        LocalServerOrientPool pool = new LocalServerOrientPool(orientDir, "testme", serverUser, serverPwd);
        Assert.assertFalse(pool.getServer().isRunning());
        try (ODatabaseDocument db = pool.acquire()) {
            Assert.assertNotNull(db);
            Assert.assertTrue(pool.getServer().isRunning());
        }
        pool.close();
        Assert.assertFalse(pool.getServer().isRunning());
    }
}
