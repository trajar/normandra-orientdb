package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LocalEmbeddedServerTest {

    File orientDist = new File("src/test/dist/orientdb-community-importers-3.0.0RC2.zip");

    File orientDir = null;

    String serverUser = "root";

    String serverPwd = "admin";

    @Before
    public void unpackDistribution() throws Exception {
        orientDir = extract(orientDist, new File("target/LocalEmbeddedServerTest").getCanonicalFile());
    }

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

    private static File extract(final File archivePath, final File destinationPath) throws IOException {
        File rootDest = null;
        try (final ZipFile zipFile = new ZipFile(archivePath)) {
            final byte[] buf = new byte[1024 * 32];
            final Enumeration<? extends ZipEntry> entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry zipEntry = entries.nextElement();
                final String entryName = zipEntry.getName().replace('\\', '/');
                if (entryName.endsWith("/")) {
                    final File destDir = new File(destinationPath, entryName);
                    if (!destDir.exists()) {
                        destDir.mkdirs();
                    }
                    if (null == rootDest) {
                        rootDest = destDir;
                    }
                } else {
                    final File destFile = new File(destinationPath, entryName);
                    if (destFile.getParentFile() != null && !destFile.getParentFile().exists()) {
                        destFile.getParentFile().mkdirs();
                    }
                    try (final OutputStream fos = new FileOutputStream(destFile)) {
                        int n;
                        try (final InputStream fis = zipFile.getInputStream(zipEntry)) {
                            while ((n = fis.read(buf)) != -1) {
                                if (n > 0) {
                                    fos.write(buf, 0, n);
                                }
                            }
                        }
                    }
                }
            }
        }
        return rootDest != null ? rootDest : destinationPath;
    }
}
