package com.bbva.dataprocessors.util.beans;

public class PersonNoDefaultConstructor {

    private String name;
    private String lastName;
    private String phone;

    public PersonNoDefaultConstructor(final String phone) {
        this.phone = phone;
    }

    public PersonNoDefaultConstructor(final String name, final String lastName) {
        this.name = name;
        this.lastName = lastName;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(final String lastName) {
        this.lastName = lastName;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(final String phone) {
        this.phone = phone;
    }
}
