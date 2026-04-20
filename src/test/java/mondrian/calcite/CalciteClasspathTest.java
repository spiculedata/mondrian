package mondrian.calcite;

import org.apache.calcite.tools.Frameworks;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class CalciteClasspathTest {
    @Test public void calciteIsOnCompileClasspath() {
        assertNotNull(Frameworks.newConfigBuilder());
    }
}
