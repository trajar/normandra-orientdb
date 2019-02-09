package org.normandra.orientdb;

import org.junit.Assert;
import org.junit.Test;
import org.normandra.GraphTest;
import org.normandra.graph.GraphManager;
import org.normandra.graph.Node;
import org.normandra.graph.SimpleNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class OrientGraphTest extends GraphTest {
    public OrientGraphTest() {
        super(new OrientHelper());
    }

    @Test
    public void testQuery() throws Exception {
        final GraphManager manager = helper.getGraph();

        final SimpleNode foo = new SimpleNode("foo");
        final SimpleNode bar = new SimpleNode("bar");
        final Node fooNode = manager.addNode(foo);
        Assert.assertNotNull(fooNode);
        final Node barNode = manager.addNode(bar);
        Assert.assertNotNull(barNode);

        Map<String, Object> params = new HashMap<>();
        params.put("myid", foo.getId());
        Collection items = manager.query(SimpleNode.class, "select from simple_node where guid = :myid", params).list();
        Assert.assertEquals(1, items.size());
        Assert.assertEquals(foo, items.iterator().next());

        params = new HashMap<>();
        params.put("myidlist", Arrays.asList(foo.getId(), bar.getId()));
        items = manager.query(SimpleNode.class, "select from simple_node where guid in :myidlist", params).list();
        Assert.assertEquals(2, items.size());
        Assert.assertTrue(items.contains(foo));
        Assert.assertTrue(items.contains(bar));
    }
}
