package mondrian.calcite;

import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class MondrianBackendTest {
    @After public void clear() { System.clearProperty("mondrian.backend"); }

    @Test public void defaultsToCalcite() {
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());
    }
    @Test public void calcitePropertyPicksCalcite() {
        System.setProperty("mondrian.backend", "calcite");
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());
    }
    @Test public void unknownFallsBackToLegacyWithWarning() {
        System.setProperty("mondrian.backend", "bogus");
        assertSame(MondrianBackend.LEGACY, MondrianBackend.current());
    }
    @Test public void caseInsensitive() {
        System.setProperty("mondrian.backend", "CALCITE");
        assertSame(MondrianBackend.CALCITE, MondrianBackend.current());
    }
}
