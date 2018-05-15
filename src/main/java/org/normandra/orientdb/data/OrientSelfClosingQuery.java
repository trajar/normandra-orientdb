package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import org.apache.commons.lang.NullArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * a query that auto-closes the query result set
 */
public class OrientSelfClosingQuery implements Iterable<ODocument>, Closeable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OrientSelfClosingQuery.class);

    private final ODatabaseDocument database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private OrientNonBlockingListener results = null;

    private Thread consumerThread = null;

    private boolean closed = false;

    public OrientSelfClosingQuery(final ODatabaseDocument db, final String query) {
        this(db, query, Collections.emptyList());
    }

    public OrientSelfClosingQuery(final ODatabaseDocument db, final String query, final Collection<?> params) {
        if (null == db) {
            throw new NullArgumentException("database");
        }
        if (null == query) {
            throw new NullArgumentException("query");
        }
        this.database = db;
        this.query = query;
        this.parameterMap = Collections.emptyMap();
        if (params != null && !params.isEmpty()) {
            this.parameterList = new ArrayList<>(params);
        } else {
            this.parameterList = Collections.emptyList();
        }
    }

    public OrientSelfClosingQuery(final ODatabaseDocument db, final String query, final Map<String, Object> params) {
        if (null == db) {
            throw new NullArgumentException("database");
        }
        if (null == query) {
            throw new NullArgumentException("query");
        }
        this.database = db;
        this.query = query;
        this.parameterList = Collections.emptyList();
        if (params != null && !params.isEmpty()) {
            this.parameterMap = new LinkedHashMap<>(params);
        } else {
            this.parameterMap = Collections.emptyMap();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            this.closeResults();
        } catch (Exception e) {
            throw new IOException("Unable to close results.");
        }
        synchronized (this) {
            this.closed = true;
        }
    }

    public boolean isClosed() {
        synchronized (this) {
            if (this.closed) {
                return true;
            }
        }

        if (database.isClosed()) {
            return true;
        }

        return false;
    }

    synchronized private Iterator<?> execute() {
        try {
            this.closeResults();
        } catch (Exception e) {
            logger.warn("Unable to close previous query results.", e);
        }

        final OrientNonBlockingListener listener = new OrientNonBlockingListener();
        final Runnable worker = () -> {
            database.activateOnCurrentThread();
            final OSQLQuery q = new OSQLAsynchQuery(query, listener);
            if (!parameterList.isEmpty()) {
                database.query(q, parameterList.toArray());
            } else if (!parameterMap.isEmpty()) {
                database.query(q, parameterMap);
            } else {
                database.query(q);
            }
        };

        this.consumerThread = new Thread(worker);
        this.consumerThread.setName(this.getClass().getSimpleName() + "-Listener");
        this.consumerThread.setDaemon(true);
        this.consumerThread.setUncaughtExceptionHandler((t, e) -> listener.offerElement(e));
        this.consumerThread.start();

        this.results = listener;
        return this.results;
    }

    synchronized private void closeResults() throws Exception {
        if (this.results != null) {
            closeElement(this.results);
            this.results = null;
        }
        if (this.consumerThread != null) {
            if (this.consumerThread.isAlive()) {
                this.consumerThread.interrupt();
            }
            this.consumerThread = null;
        }
    }

    private static void closeElement(final Object obj) throws Exception {
        if (null == obj) {
            return;
        }
        if (obj instanceof Closeable) {
            ((Closeable) obj).close();
        } else if (obj instanceof AutoCloseable) {
            ((AutoCloseable) obj).close();
        }
    }

    @Override
    public Iterator<ODocument> iterator() {
        // execute query
        final Iterator<?> results = this.execute();
        if (null == results) {
            return Collections.emptyIterator();
        }

        // wrap with closing iterator
        return new Iterator<ODocument>() {
            @Override
            public boolean hasNext() {
                if (isClosed()) {
                    return false;
                }

                if (!results.hasNext()) {
                    try {
                        close();
                    } catch (Exception e) {
                        logger.warn("Unable to close results.", e);
                    }
                    return false;
                }

                return true;
            }

            @Override
            public ODocument next() {
                if (isClosed()) {
                    throw new IllegalStateException("Query is closed.");
                }

                final Object obj = results.next();
                if (null == obj) {
                    return null;
                }

                if (obj instanceof ODocument) {
                    return (ODocument) obj;
                } else if (obj instanceof ORecord) {
                    return database.load((ORecord) obj);
                } else if (obj instanceof OIdentifiable) {
                    return database.load(((OIdentifiable) obj).getIdentity());
                } else if (obj instanceof ORID) {
                    return database.load((ORID) obj);
                }

                throw new IllegalStateException("Unexpected document type [" + obj.getClass() + "].");
            }
        };
    }

    private class OrientNonBlockingListener implements OCommandResultListener, Iterator<Object>, Closeable {
        private final BlockingQueue<Object> queue = new ArrayBlockingQueue<>(250);

        private Object next = null;

        private boolean done = false;

        private boolean needsFetch = true;

        @Override
        public boolean result(final Object record) {
            if (null == record) {
                return false;
            }

            if (isClosed()) {
                return false;
            }

            return this.offerElement(record);
        }

        @Override
        public void end() {
            this.endService();
        }

        @Override
        public Object getResult() {
            return null;
        }

        @Override
        public void close() {
            if (!this.isDone()) {
                this.endService();
            }
        }

        private boolean endService() {
            synchronized (this) {
                this.done = true;
            }

            return this.offerElement(new EndOfServiceElement());
        }

        synchronized private boolean isDone() {
            return this.done;
        }

        private boolean offerElement(final Object item) {
            final long fetchStartMs = System.currentTimeMillis();
            final long maxWaitMs = getTimeOutDelay();
            while (!isClosed() && !isDone()) {
                try {
                    if (queue.offer(item, 100, TimeUnit.MILLISECONDS)) {
                        return true;
                    }
                } catch (final InterruptedException e) {
                    logger.trace("Unable to offer element in non-blocking queue.", e);
                    return false;
                }
                if (maxWaitMs > 0 && System.currentTimeMillis() - fetchStartMs > maxWaitMs) {
                    logger.debug("Waited more than [" + maxWaitMs + "] msec to add item to queue - offer aborted.");
                    return false;
                }
            }
            return false;
        }

        @Override
        public boolean hasNext() {
            if (this.needsFetch) {
                if (!this.fetch()) {
                    return false;
                }
            }

            return this.next != null;
        }

        @Override
        public Object next() {
            if (this.needsFetch) {
                if (!this.fetch()) {
                    return null;
                }
            }

            final Object document = this.next;
            this.next = null;
            this.needsFetch = true;
            return document;
        }

        private boolean fetch() {
            this.next = null;
            this.needsFetch = true;
            final long fetchStartMs = System.currentTimeMillis();
            final long maxWaitMs = getTimeOutDelay();
            while (!this.queue.isEmpty() || (!isClosed() && !isDone())) {
                try {
                    final Object item = this.queue.poll(100, TimeUnit.MILLISECONDS);
                    if (item instanceof EndOfServiceElement) {
                        this.next = null;
                        this.needsFetch = false;
                        return false;
                    } else if (item instanceof Throwable) {
                        throw new IllegalStateException("Unable to get next element from query.", (Throwable) item);
                    } else if (item != null) {
                        this.next = item;
                        this.needsFetch = false;
                        return true;
                    } else if (maxWaitMs > 0 && System.currentTimeMillis() - fetchStartMs > maxWaitMs) {
                        logger.debug("Waited more than [" + maxWaitMs + "] msec for item from queue - fetch aborted.");
                        return false;
                    }
                } catch (final InterruptedException e) {
                    logger.debug("Unable to poll non-blocking queue.", e);
                    return false;
                }
            }
            return false;
        }
    }

    private static class EndOfServiceElement {
        private final UUID guid = UUID.randomUUID();

        private final long time = System.currentTimeMillis();
    }

    private static long getTimeOutDelay() {
        final String prop = System.getProperty("query.nonBlocking.timeOut");
        if (null == prop || prop.trim().isEmpty()) {
            return 1000 * 60;
        }

        try {
            return Long.parseLong(prop);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to parse timeout value of [" + prop + "].", e);
        }
    }
}
