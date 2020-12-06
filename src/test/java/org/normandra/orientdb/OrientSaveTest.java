package org.normandra.orientdb;

import org.junit.Assert;
import org.junit.Test;
import org.normandra.DatabaseConstruction;
import org.normandra.EntityManager;
import org.normandra.SaveTest;
import org.normandra.entities.Address;
import org.normandra.entities.CatEntity;
import org.normandra.entities.DogEntity;
import org.normandra.entities.StoreEntity;
import org.normandra.meta.DatabaseMeta;
import org.normandra.meta.DatabaseMetaBuilder;
import org.normandra.meta.EntityMeta;
import org.normandra.orientdb.data.OrientDatabase;
import org.normandra.orientdb.data.OrientDatabaseSession;

public class OrientSaveTest extends SaveTest {
    public OrientSaveTest() {
        super(new OrientHelper());
    }

    @Test
    public void testRefreshWithNewSchema() throws Exception {
        OrientDatabase database = (OrientDatabase) this.helper.getDatabase();
        EntityManager manager = this.helper.getManager();
        OrientDatabaseSession session = (OrientDatabaseSession) manager.getSession();

        session.database().activateOnCurrentThread();
        StoreEntity store = new StoreEntity("big bobs", new Address("bob street", "centervelle", 123));
        DogEntity fluffy = new DogEntity("fluffy", 4);
        manager.save(fluffy);
        manager.save(store);
        manager.clear();

        DatabaseMeta updatedMeta = new DatabaseMetaBuilder().withClasses(CatEntity.class, DogEntity.class).create();
        database.refreshWith(updatedMeta, DatabaseConstruction.FORCE_SCHEMA);

        session.database().activateOnCurrentThread();
        Assert.assertEquals(fluffy, manager.get(DogEntity.class, fluffy.getId()));
        Exception error = null;
        try {
            manager.get(StoreEntity.class, store.getId());
        } catch (final Exception e) {
            e.printStackTrace();
            error = e;
        }
        Assert.assertNotNull(error);
    }

    @Test
    public void testSchema() throws Exception {
        // DogEntity.class, CatEntity.class, ZooEntity.class,
        // StoreEntity.class, ParkingLotEntity.class, ClassEntity.class, StudentEntity.class
        OrientDatabase database = (OrientDatabase) this.helper.getDatabase();
        EntityManager manager = this.helper.getManager();
        OrientDatabaseSession session = (OrientDatabaseSession) manager.getSession();

        Assert.assertTrue(database.hasEntity("animal"));
        Assert.assertTrue(database.hasEntity("store_entity"));

        EntityMeta dogMeta = database.getMeta().getEntity("animal");
        Assert.assertTrue(database.hasProperty(dogMeta, "num_barks"));
        Assert.assertTrue(database.removeProperty("animal", "num_barks"));
        Assert.assertFalse(database.hasProperty(dogMeta, "num_barks"));

        // even after removing the property from schema, we should be able to save it
        // but it will only exist in property fields, outside larger scheme
        session.database().activateOnCurrentThread();
        DogEntity fluffy = new DogEntity("fluffy", 5);
        manager.save(fluffy);
        manager.clear();
        Assert.assertFalse(database.hasProperty("animal", "num_barks"));
        session.database().activateOnCurrentThread();
        Assert.assertEquals(fluffy, manager.get(DogEntity.class, fluffy.getId()));

        Assert.assertFalse(database.hasIndex("animal", "id"));
        Assert.assertTrue(database.hasIndex("animal", "animal.id"));
        Assert.assertTrue(database.removeIndex("animal", "animal.id"));
        Assert.assertFalse(database.hasIndex("animal", "animal.id"));

        // removing the index will ensure we can no longer query by id
        session.database().activateOnCurrentThread();
        manager.clear();
        DogEntity matching = null;
        try {
            matching = manager.get(DogEntity.class, fluffy.getId());
        } catch (final Exception e) {

        }
        Assert.assertNull(matching);
    }
}