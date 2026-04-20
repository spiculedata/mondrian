package mondrian.calcite;

import org.apache.calcite.tools.Frameworks;

/** Forces {@link Frameworks} onto the production classpath (see Task 1 of the Calcite foundations plan). */
final class ClasspathProbe {
    static final Class<?> REF = Frameworks.class;
    private ClasspathProbe() {}
}
