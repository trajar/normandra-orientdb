package org.normandra.orientdb;

import org.junit.Assert;
import org.junit.Test;
import org.normandra.DatabaseQuery;
import org.normandra.EntityManager;
import org.normandra.PropertyQuery;
import org.normandra.QueryTest;
import org.normandra.entities.AnimalEntity;
import org.normandra.entities.DogEntity;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class OrientQueryTest extends QueryTest {
    public OrientQueryTest() {
        super(new OrientHelper());
    }

    @Test
    public void testProperty() throws Exception {
        final EntityManager manager = helper.getManager();

        final DogEntity dog = new DogEntity("sophi", 2);
        manager.save(dog);
        Assert.assertNotNull(dog.getId());
        manager.clear();

        PropertyQuery query = manager.query("select name, num_barks from animal where num_barks > 0");
        Assert.assertNotNull(query);
        Collection<Map<String, Object>> list = query.list();
        Assert.assertEquals(1, list.size());
        Map<String, Object> properties = list.iterator().next();
        Assert.assertEquals(2, properties.size());
        Assert.assertEquals("sophi", properties.get("name"));
    }

    @Test
    public void testSimple() throws Exception {
        final EntityManager manager = helper.getManager();

        final DogEntity dog = new DogEntity("sophi", 2);
        manager.save(dog);
        Assert.assertNotNull(dog.getId());
        manager.clear();

        final Map<String, Object> params = new TreeMap<>();
        params.put("id", dog.getId());

        final DatabaseQuery<DogEntity> queryByTable = manager.query(DogEntity.class, "select from animal where id = :id", params);
        Assert.assertNotNull(queryByTable);
        Collection<?> elements = queryByTable.list();
        Assert.assertNotNull(queryByTable);
        Assert.assertEquals(1, elements.size());

        final DatabaseQuery<AnimalEntity> queryNamed = manager.query(AnimalEntity.class, "select from animal where id = :id", params);
        Assert.assertNotNull(queryNamed);
        elements = queryNamed.list();
        Assert.assertNotNull(queryNamed);
        Assert.assertEquals(1, elements.size());
    }
}
