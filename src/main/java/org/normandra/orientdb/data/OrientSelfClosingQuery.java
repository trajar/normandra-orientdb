package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.commons.lang.NullArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * a query that auto-closes the query result set
 */
public class OrientSelfClosingQuery implements Iterable<ODocument>, Closeable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OrientSelfClosingQuery.class);

    private final ODatabaseDocument database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private Iterator<?> results = null;

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
    }

    synchronized private Iterator<?> execute() {
        try {
            this.closeResults();
        } catch (Exception e) {
            logger.warn("Unable to close previous query results.", e);
        }
        final OSQLQuery q = new OSQLSynchQuery(this.query);
        final Iterable<?> items;
        if (!parameterList.isEmpty()) {
            items = database.query(q, this.parameterList.toArray());
        } else if (!parameterMap.isEmpty()) {
            items = database.query(q, this.parameterMap);
        } else {
            items = database.query(q);
        }
        this.results = items.iterator();
        return this.results;
    }

    synchronized private void closeResults() throws Exception {
        if (this.results != null) {
            closeElement(this.results);
            this.results = null;
        }
    }

    private static void closeElement(final Object obj) throws Exception {
        if (null == obj) {
            return;
        }
        if (obj instanceof Closeable) {
            ((Closeable) obj).close();
        }
        if (obj instanceof AutoCloseable) {
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
                if (results.hasNext()) {
                    return true;
                } else {
                    try {
                        closeElement(results);
                    } catch (Exception e) {
                        logger.warn("Unable to close results.", e);
                    }
                    return false;
                }
            }

            @Override
            public ODocument next() {
                final Object obj = results.next();
                if (null == obj) {
                    return null;
                }
                if (obj instanceof ODocument) {
                    return (ODocument) obj;
                }
                if (obj instanceof ORecord) {
                    return database.load((ORecord) obj);
                }
                if (obj instanceof OIdentifiable) {
                    return database.load(((OIdentifiable) obj).getIdentity());
                }
                if (obj instanceof ORID) {
                    return database.load((ORID) obj);
                }
                throw new IllegalStateException("Unexpected document type [" + obj.getClass() + "].");
            }
        };
    }

}
