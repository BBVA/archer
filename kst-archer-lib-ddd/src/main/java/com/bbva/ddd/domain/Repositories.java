package com.bbva.ddd.domain;

import com.bbva.ddd.domain.changelogs.Repository;

import java.util.Map;

class Repositories {

    private static Repositories instance = new Repositories();
    private Map<String, Repository> repositories;

    private Repositories() {

    }

    void setRepositories(Map<String, Repository> repositories) {
        this.repositories = repositories;
    }

    static Repositories getInstance() {
        return instance;
    }

    static Repository get(String baseName) {
        return instance.repositories.get(baseName);
    }
}
