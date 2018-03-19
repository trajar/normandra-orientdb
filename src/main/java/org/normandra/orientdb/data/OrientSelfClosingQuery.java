package org.normandra.orientdb.data;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.apache.commons.lang.NullArgumentException;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * a query that auto-closes the query result set
 *
 * @date 3/13/18.
 */
public class OrientSelfClosingQuery implements Iterable<OResult>, Closeable, AutoCloseable {
    private final ODatabaseDocument database;

    private final String query;

    private final List<Object> parameterList;

    private final Map<String, Object> parameterMap;

    private OResultSet results = null;

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
        this.closeResults();
    }

    synchronized public OResultSet execute() {
        this.closeResults();
        if (!parameterList.isEmpty()) {
            this.results = database.query(this.query, this.parameterList.toArray());
        } else if (!parameterMap.isEmpty()) {
            this.results = database.query(this.query, this.parameterMap);
        } else {
            this.results = database.query(this.query);
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
    public Iterator<OResult> iterator() {
        // execute query
        final OResultSet results = this.execute();
        if (null == results) {
            return Collections.emptyIterator();
        }

        // wrap with closing iterator
        return new Iterator<OResult>() {
            @Override
            public boolean hasNext() {
                if (results.hasNext()) {
                    return true;
                } else {
                    results.close();
                    return false;
                }
            }

            @Override
            public OResult next() {
                return results.next();
            }
        };
    }

}
