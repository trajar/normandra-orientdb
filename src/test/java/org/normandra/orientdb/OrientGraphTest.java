package org.normandra.orientdb;

import org.junit.Assert;
import org.junit.Test;
import org.normandra.GraphTest;
import org.normandra.NormandraException;
import org.normandra.Transaction;
import org.normandra.graph.*;

import java.util.*;
import java.util.stream.Collectors;

public class OrientGraphTest extends GraphTest {
    public OrientGraphTest() {
        super(new OrientHelper());
//      super(new OrientHelper(true, true));
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

    @Test
    public void testTransaction() throws Exception {
        final GraphManager manager = helper.getGraph();

        final SimpleNode foo = new SimpleNode("foo");
        final SimpleNode bar = new SimpleNode("bar");

        final List<Node> nodes = new ArrayList<>();
        manager.withTransaction(tx -> {
            final Node fooNode = manager.addNode(foo);
            Assert.assertNotNull(fooNode);
            final Node barNode = manager.addNode(bar);
            Assert.assertNotNull(barNode);
            tx.success();
            nodes.add(fooNode);
            nodes.add(barNode);
        });

        Exception foundException = null;
        try {
            manager.withTransaction(tx -> {
                try (final Transaction nestedTx = manager.beginTransaction()) {
                    nodes.add(manager.addNode(new SimpleNode("other1")));
                    nestedTx.failure();
                }
                nodes.add(manager.addNode(new SimpleNode("bad egg")));
                tx.success();
            });
        } catch (final Exception e) {
            foundException = e;
        }
        Assert.assertNotNull(foundException);

        final List<Node> filteredNodes = nodes.stream().filter((x) -> {
            try {
                return x != null && x.getEntity() != null;
            } catch (NormandraException e) {
                e.printStackTrace();
                return false;
            }
        }).collect(Collectors.toList());
        Assert.assertTrue(!filteredNodes.isEmpty());
        manager.clear();
        int numSaved = 0;
        for (final Node node : filteredNodes) {
            final SimpleNode entity = (SimpleNode) node.getEntity();
            if (manager.getNode(SimpleNode.class, entity.getId()) != null) {
                numSaved++;
            }
        }
        Assert.assertEquals(2, numSaved);
    }

    @Test
    public void testPerformance() throws Exception {
        final GraphManager manager = helper.getGraph();
        final long testStart = System.currentTimeMillis();
        final List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            long start = System.currentTimeMillis();
            manager.withTransaction(tx -> {
                Node node = manager.addNode(new SimpleNode("node-" + UUID.randomUUID().toString()));
                Assert.assertNotNull(node);
                nodes.add(node);
                Node other = manager.addNode(new SimpleNode("node-" + UUID.randomUUID().toString()));
                Assert.assertNotNull(other);
                nodes.add(other);
                for (int j = 0; j < 25; j++) {
                    Edge edge = node.createEdge(other, new SimpleEdge("edge-" + UUID.randomUUID().toString()));
                    Assert.assertNotNull(edge);
                }
                tx.success();
            });
            long duration = System.currentTimeMillis() - start;
            System.out.println("Added nodes [" + nodes.get(nodes.size() - 1) + "], [" + nodes.get(nodes.size() - 1) + "] which took [" + duration + "] msec.");
        }
        final long testDuration = System.currentTimeMillis() - testStart;
        System.out.println("Added [" + nodes.size() + "] which took [" + testDuration + "] msec.");
    }
}
