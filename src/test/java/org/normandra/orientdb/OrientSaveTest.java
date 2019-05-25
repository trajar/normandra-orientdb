package org.normandra.orientdb;

import org.normandra.SaveTest;

public class OrientSaveTest extends SaveTest {
    public OrientSaveTest() {
        super(new OrientHelper());
    }
}
