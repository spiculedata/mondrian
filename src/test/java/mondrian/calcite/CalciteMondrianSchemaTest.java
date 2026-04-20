/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
// Copyright (C) 2026 Spicule
// All Rights Reserved.
*/
package mondrian.calcite;

import mondrian.test.FoodMartHsqldbBootstrap;

import org.apache.calcite.schema.SchemaPlus;
import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link CalciteMondrianSchema}: confirm the adapter exposes
 * the FoodMart fact and dimension tables reflected from a HSQLDB DataSource.
 */
public class CalciteMondrianSchemaTest {

    @BeforeClass
    public static void bootFoodMart() {
        FoodMartHsqldbBootstrap.ensureExtracted();
    }

    private static DataSource foodmartDs() {
        jdbcDataSource ds = new jdbcDataSource();
        ds.setDatabase("jdbc:hsqldb:file:target/foodmart/foodmart;readonly=true");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    /**
     * HSQLDB 1.8 stores unquoted identifiers upper-case; the FoodMart
     * fixture script creates tables using lowercase quoted names, so they
     * remain lowercase. Either way, find the fact table case-insensitively.
     */
    private static org.apache.calcite.schema.Table findTableCi(
        SchemaPlus schema, String name)
    {
        for (String t : schema.getTableNames()) {
            if (t.equalsIgnoreCase(name)) {
                return schema.getTable(t);
            }
        }
        return null;
    }

    @Test
    public void adapterExposesFactTable() {
        CalciteMondrianSchema adapter =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        assertNotNull(
            "expected sales_fact_1997 in reflected schema; saw "
                + adapter.schema().getTableNames(),
            findTableCi(adapter.schema(), "sales_fact_1997"));
    }

    @Test
    public void adapterExposesDimensionTable() {
        CalciteMondrianSchema adapter =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        assertNotNull(
            "expected time_by_day in reflected schema; saw "
                + adapter.schema().getTableNames(),
            findTableCi(adapter.schema(), "time_by_day"));
    }

    @Test
    public void unknownTableReturnsNull() {
        CalciteMondrianSchema adapter =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        assertNull(adapter.schema().getTable("does_not_exist"));
        assertNull(adapter.schema().getTable("DOES_NOT_EXIST"));
    }

    @Test
    public void rootSchemaContainsNamedSubschema() {
        CalciteMondrianSchema adapter =
            new CalciteMondrianSchema(foodmartDs(), "foodmart");
        assertNotNull(adapter.root().getSubSchema("foodmart"));
        assertNotNull(adapter.root());
    }

    @Test
    public void nullDataSourceThrows() {
        try {
            new CalciteMondrianSchema(null, "foodmart");
            fail("expected IllegalArgumentException for null DataSource");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }
}

// End CalciteMondrianSchemaTest.java
