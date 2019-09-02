package com.bbva.dataprocessors.util.beans;

public class PersonWithoutGetters {

    private String name;
    private String lastName;
    private String phone;
    
    public PersonWithoutGetters(final String phone) {
        this.phone = phone;
    }

    public PersonWithoutGetters(final String name, final String lastName) {
        this.name = name;
        this.lastName = lastName;
    }
}
