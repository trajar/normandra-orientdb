package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * a local pool that is tied to a local/embedded server instance
 *
 * @date 2/17/18.
 */
public class LocalServerOrientPool implements OrientPool {
    private static final Logger logger = LoggerFactory.getLogger(LocalServerOrientPool.class);

    private final LocalEmbeddedServer localServer;

    private final DynamicOrientPool pool;

    private final String database;

    private final String username;

    private final String password;

    private int startTimeoutMsec = 1000 * 60 * 5;

    private boolean firstTime = true;

    public LocalServerOrientPool(final File orientDir, final String database, final String user, final String pwd) {
        this(orientDir, database, user, pwd, false);
    }

    public LocalServerOrientPool(final File orientDir, final String database, final String user, final String pwd, final boolean separateProcess) {
        this(orientDir, user, pwd, database, user, pwd, separateProcess);
    }

    public LocalServerOrientPool(
            final File orientDir, final String serverUser, final String serverPwd,
            final String database, final String databaseUser, final String databasePwd) {
        this(orientDir, serverUser, serverPwd, database, databaseUser, databasePwd, false);
    }

    public LocalServerOrientPool(
            final File orientDir, final String serverUser, final String serverPwd,
            final String database, final String databaseUser, final String databasePwd,
            final boolean separateProcess) {
        this.localServer = new LocalEmbeddedServer(orientDir, serverUser, serverPwd, separateProcess);
        this.pool = new DynamicOrientPool(this.localServer.getBinaryUrl(), database, databaseUser, databasePwd);
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
            boolean exists = false;
            try {
                // ensure database exists
                final Map<String, ?> dbconfig = this.localServer.getDatabase(this.database);
                if (dbconfig != null && dbconfig.size() > 0) {
                    exists = true;
                }
            } catch (final Exception e) {
                logger.debug("Unable to query local database [" + this.database + "].", e);
            }
            if (!exists) {
                try {
                    this.localServer.createDatabase(this.database);
                } catch (IOException e) {
                    logger.warn("Unaboe to create local database [" + this.database + "].", e);
                }
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
