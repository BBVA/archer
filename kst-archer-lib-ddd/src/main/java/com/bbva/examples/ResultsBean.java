package com.bbva.examples;

import java.util.Objects;

public class ResultsBean {

    private Integer code;
    private String description;
    private Object data;

    public ResultsBean() {
    }

    public ResultsBean(final Integer code, final String description) {

        this.code = code;
        this.description = description;
    }

    public ResultsBean(Integer code, String description, Object data) {
        this.code = code;
        this.description = description;
        this.data = data;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(final Integer code) {
        this.code = code;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(final String description) {
        this.description = description;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "result{" + "code='" + code + '\'' + ", description='" + description + '\'' + ", data='"
                + data.toString() + '\'' + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ResultsBean that = (ResultsBean) o;
        return Objects.equals(code, that.code) && Objects.equals(description, that.description)
                && Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(code, description, data);
    }
}