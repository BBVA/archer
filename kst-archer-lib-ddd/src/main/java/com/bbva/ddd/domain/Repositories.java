package com.bbva.ddd.domain;

import com.bbva.ddd.domain.changelogs.Repository;

import java.util.Map;

final class Repositories {

    private static final Repositories instance = new Repositories();
    private Map<String, Repository> repositories;

    private Repositories() {

    }

    void setRepositories(final Map<String, Repository> repositories) {
        this.repositories = repositories;
    }

    static Repositories getInstance() {
        return instance;
    }

    static Repository get(final String baseName) {
        return instance.repositories.get(baseName);
    }
}
