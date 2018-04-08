package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

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

    private final String url;

    private final String database;

    private final String username;

    private final String password;

    public DynamicOrientPool(final String url, final String database, final String user, final String pwd) {
        this.url = url;
        this.database = database;
        this.username = user;
        this.password = pwd;
    }

    @Override
    public ODatabaseDocument acquire() {
        this.checkOpenedConnections();
        final ODatabaseDocument session = new ODatabaseDocumentTx(OrientUtils.url(url, database)).open(this.username, this.password);
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
    }

    synchronized private void checkOpenedConnections() {
        for (final ODatabaseDocument session : new ArrayList<>(this.opened)) {
            if (session.isClosed()) {
                this.opened.remove(session);
            }
        }
    }
}
