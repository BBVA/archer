package com.bbva.ddd.domain;

import com.bbva.ddd.domain.changelogs.repository.Repository;

import java.util.Map;

/**
 * Store a map of repositories by name
 */
public final class Repositories {

    private static final Repositories instance = new Repositories();
    private Map<String, Repository> repositories;

    /**
     * Set the respositories
     *
     * @param repositories map of repositories
     */
    void setRepositories(final Map<String, Repository> repositories) {
        this.repositories = repositories;
    }

    /**
     * Get respositories instance
     *
     * @return the instance
     */
    static Repositories getInstance() {
        return instance;
    }

    /**
     * Get specific repository by name
     *
     * @param baseName repository name
     * @return repository
     */
    public static Repository get(final String baseName) {
        return instance.repositories.get(baseName);
    }
}
