package com.ayoubom.kafka.utils;


import java.util.Objects;

public class KeyValueBean {

    private String key;
    private Long value;

    public KeyValueBean() {}

    public KeyValueBean(final String key, final Long value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {

        return key;
    }

    public void setKey(final String key) {
        this.key = key;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(final Long value) {
        this.value = value;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final KeyValueBean that = (KeyValueBean) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "KeyValueBean{" +
                "key='" + key + '\'' +
                ", value=" + value +
                '}';
    }
}