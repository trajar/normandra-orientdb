package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * a pool that creates connections on demand
 *
 * @date 3/21/18.
 */
public class DynamicOrientPool implements OrientPool {

    private final Collection<ODatabaseDocument> opened = new CopyOnWriteArraySet<>();

    private final OrientDB orientdb;

    private final String database;

    private final String username;

    private final String password;

    public DynamicOrientPool(final String url, final String database, final String user, final String pwd) {
        this(url, user, pwd, database, user, pwd);
    }

    public DynamicOrientPool(final String url, final String serveruser, final String serverpwd, final String database, final String user, final String pwd) {
        this.orientdb = new OrientDB(url, serveruser, serverpwd, OrientDBConfig.defaultConfig());
        this.database = database;
        this.username = user;
        this.password = pwd;
    }

    @Override
    public ODatabaseDocument acquire() {
        this.checkOpenedConnections();
        final ODatabaseDocument session = this.orientdb.open(this.database, this.username, this.password);
        if (null == session) {
            return null;
        }
        session.activateOnCurrentThread();
        this.opened.add(session);
        return session;
    }

    @Override
    synchronized public void close() {
        this.checkOpenedConnections();
        for (final ODatabaseDocument session : new ArrayList<>(this.opened)) {
            if (!session.isClosed()) {
                session.activateOnCurrentThread();
                session.close();
            }
        }
        this.opened.clear();
        this.orientdb.close();
    }

    synchronized private void checkOpenedConnections() {
        for (final ODatabaseDocument session : new ArrayList<>(this.opened)) {
            if (session.isClosed()) {
                this.opened.remove(session);
            }
        }
    }
}
