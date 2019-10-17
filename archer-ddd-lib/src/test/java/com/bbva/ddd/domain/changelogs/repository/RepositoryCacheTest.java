package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.commands.producers.records.PersonalData;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({CachedProducer.class, Repository.class})
public class RepositoryCacheTest {

    @DisplayName("Create repository cache and get state ok")
    @Test
    public void createRepositoryAndGetState() {
        final RepositoryCache<PersonalData> repositoryCache = new RepositoryCache<>();

        final PersonalData data = repositoryCache.getCurrentState("personal-data", "key");

        Assertions.assertAll("RepositoryCache",
                () -> Assertions.assertNull(data)
        );
    }

    @DisplayName("Create repository cache and get record ok")
    @Test
    public void getRecord() {
        final RepositoryCache<PersonalData> repositoryCache = new RepositoryCache<>();

        final RepositoryCache.Record data = repositoryCache.getRecord("key");

        Assertions.assertAll("RepositoryCache",
                () -> Assertions.assertNull(data)
        );
    }


    @DisplayName("Create repository cache and update state ok")
    @Test
    public void createRepositoryAndUpdateState() {
        final RepositoryCache<PersonalData> repositoryCache = new RepositoryCache<>();

        repositoryCache.updateState("key", new PersonalData(), null);

        repositoryCache.getCurrentState("personal-data", "key");
        repositoryCache.updateState("key", new PersonalData(), null);
        final RepositoryCache.Record data = repositoryCache.getRecord("key");

        Assertions.assertAll("RepositoryCache",
                () -> Assertions.assertNotNull(data)
        );
    }

    @DisplayName("Create repository cache and update state ok")
    @Test
    public void createRepositoryAndUpdateState2() {
        final RepositoryCache<PersonalData> repositoryCache = new RepositoryCache<>();

        repositoryCache.updateState("key", new PersonalData(), null);

        repositoryCache.getCurrentState("personal-data", "key");
        repositoryCache.updateState("key", null, null);
        final RepositoryCache.Record data = repositoryCache.getRecord("key");

        Assertions.assertAll("RepositoryCache",
                () -> Assertions.assertNotNull(data)
        );
    }

    @DisplayName("Create repository cache and update state ok")
    @Test
    public void createRepositoryAndUpdateState3() {
        final RepositoryCache<PersonalData> repositoryCache = new RepositoryCache<>();

        repositoryCache.updateState("key", null, null);

        repositoryCache.getCurrentState("personal-data", "key");
        repositoryCache.updateState("key", new PersonalData(), null);
        final RepositoryCache.Record data = repositoryCache.getRecord("key");

        Assertions.assertAll("RepositoryCache",
                () -> Assertions.assertNotNull(data)
        );
    }
}
