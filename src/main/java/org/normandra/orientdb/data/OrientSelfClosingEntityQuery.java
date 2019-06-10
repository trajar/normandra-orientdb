package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.commons.lang.NullArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * a query that auto-closes the query result set
 */
public class OrientSelfClosingEntityQuery implements Iterable<ODocument>, Closeable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(OrientSelfClosingEntityQuery.class);

    private final ODatabaseDocument database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private OResultSet results = null;

    private final boolean namedParameters;

    private boolean closed = false;

    public OrientSelfClosingEntityQuery(final ODatabaseDocument db, final String query) {
        this(db, query, Collections.emptyList());
    }

    public OrientSelfClosingEntityQuery(final ODatabaseDocument db, final String query, final Collection<?> params) {
        if (null == db) {
            throw new NullArgumentException("database");
        }
        if (null == query) {
            throw new NullArgumentException("query");
        }
        this.database = db;
        this.query = query;
        this.namedParameters = false;
        this.parameterMap = Collections.emptyMap();
        if (params != null && !params.isEmpty()) {
            this.parameterList = new ArrayList<>(params);
        } else {
            this.parameterList = Collections.emptyList();
        }
    }

    public OrientSelfClosingEntityQuery(final ODatabaseDocument db, final String query, final Map<String, Object> params) {
        if (null == db) {
            throw new NullArgumentException("database");
        }
        if (null == query) {
            throw new NullArgumentException("query");
        }
        this.database = db;
        this.query = query;
        this.parameterList = Collections.emptyList();
        this.namedParameters = true;
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

    synchronized private OResultSet execute() {
        try {
            this.closeResults();
        } catch (Exception e) {
            logger.warn("Unable to close previous query results.", e);
        }

        this.results = null;
        if (!parameterList.isEmpty()) {
            final List<Object> packed = parameterList.stream()
                    .map(OrientUtils::packPrimitive)
                    .collect(Collectors.toList());
            this.results = this.database.query(this.query, packed.toArray());
        } else if (namedParameters || !parameterMap.isEmpty()) {
            final Map<String, Object> packed = new LinkedHashMap<>();
            for (final Map.Entry<String, Object> entry : parameterMap.entrySet()) {
                packed.put(entry.getKey(), OrientUtils.packPrimitive(entry.getValue()));
            }
            this.results = this.database.query(this.query, packed);
        } else {
            this.results = this.database.query(this.query);
        }
        return this.results;
    }

    synchronized private void closeResults() {
        if (this.results != null) {
            this.results.close();
            this.results = null;
        }
    }

    @Override
    public Iterator<ODocument> iterator() {
        // execute query
        final Iterator<OResult> results = this.execute();
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

                final OResult obj = results.next();
                if (null == obj) {
                    return null;
                }

                if (obj.getElement().isPresent()) {
                    final OElement element = obj.getElement().get();
                    if (element instanceof ODocument) {
                        return (ODocument) element;
                    } else {
                        return database.load(element);
                    }
                } else if (obj.getRecord().isPresent()) {
                    final ORecord record = obj.getRecord().get();
                    if (record instanceof ODocument) {
                        return (ODocument) record;
                    } else {
                        return database.load(record);
                    }
                } else if (obj.getIdentity().isPresent()) {
                    return database.load(obj.getIdentity().get());
                }

                throw new IllegalStateException("Unexpected document type [" + obj + "].");
            }
        };
    }
}
