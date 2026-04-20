package mondrian.calcite;

public enum MondrianBackend {
    LEGACY, CALCITE;

    public static final String PROPERTY = "mondrian.backend";

    public static MondrianBackend current() {
        String raw = System.getProperty(PROPERTY, "legacy");
        try { return MondrianBackend.valueOf(raw.toUpperCase(java.util.Locale.ROOT)); }
        catch (IllegalArgumentException ex) { return LEGACY; }
    }

    public boolean isCalcite() { return this == CALCITE; }
}
