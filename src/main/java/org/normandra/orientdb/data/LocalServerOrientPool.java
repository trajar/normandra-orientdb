package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * a local pool that is tied to a local/embedded server instance
 *
 * @date 2/17/18.
 */
public class LocalServerOrientPool implements OrientPool {
    private static final Logger logger = LoggerFactory.getLogger(LocalServerOrientPool.class);

    private final LocalEmbeddedServer localServer;

    private final ODatabasePool pool;

    private final String database;

    private final String username;

    private final String password;

    private int startTimeoutMsec = 1000 * 60 * 5;

    private boolean firstTime = true;

    public LocalServerOrientPool(final File orientDir, final String database, final String user, final String pwd) {
        this(orientDir, user, pwd, database, user, pwd);
    }

    public LocalServerOrientPool(
            final File orientDir, final String serverUser, final String serverPwd,
            final String database, final String databaseUser, final String databasePwd) {
        this.localServer = new LocalEmbeddedServer(orientDir, serverUser, serverPwd);
        this.pool = new ODatabasePool(this.localServer.getBinaryUrl(), database, databaseUser, databasePwd);
        this.database = database;
        this.username = databaseUser;
        this.password = databasePwd;
    }

    public LocalEmbeddedServer getServer() {
        return this.localServer;
    }

    public int getStartTimeoutMsec() {
        return startTimeoutMsec;
    }

    public void setStartTimeoutMsec(int startTimeoutMsec) {
        this.startTimeoutMsec = startTimeoutMsec;
    }

    public void start() throws Exception {
        this.localServer.startIfNotRunning();
        if (!this.localServer.waitUntilReady(this.startTimeoutMsec)) {
            throw new IllegalStateException("Unable to confirm local server started.");
        }
    }

    @Override
    public ODatabaseDocument acquire() {
        if (this.firstTime) {
            try {
                this.start();
                this.firstTime = false;
            } catch (final Exception e) {
                logger.warn("Unable to start local database server.", e);
            }
            try {
                // ensure database exists
                try (final OrientDB db = new OrientDB(this.localServer.getBinaryUrl(), this.username, this.password, OrientDBConfig.defaultConfig())) {
                    db.createIfNotExists(this.database, ODatabaseType.PLOCAL);
                }
            } catch (final Exception e) {
                logger.warn("Unable to create local database [" + this.database + "].", e);
            }
        }
        return this.pool.acquire();
    }

    @Override
    public void close() {
        this.pool.close();
        try {
            this.localServer.stop();
        } catch (final Exception e) {
            logger.warn("Unable to stop local server.", e);
        }
    }
}
