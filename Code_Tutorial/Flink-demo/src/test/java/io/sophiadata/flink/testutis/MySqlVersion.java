package io.sophiadata.flink.testutis;

/** (@sophiadata) (@date 2022/12/20 15:45). */
public enum MySqlVersion {
    V8_0("8.0");

    private String version;

    MySqlVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "MySqlVersion{" + "version='" + version + '\'' + '}';
    }
}
