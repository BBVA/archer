package com.bbva.ddd.util;

import com.bbva.ddd.domain.AutoConfiguredHandler;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

@RunWith(JUnit5.class)
public class AnnotationUtilTest {

    @DisplayName("get all annotated classes ok")
    @Test
    public void getAllAnnotationsOk() {
        final List<Class> classes = AnnotationUtil.findAllAnnotatedClasses(RunWith.class);

        Assertions.assertFalse(classes.isEmpty());
    }

    @DisplayName("get all annotated classes ok")
    @Test
    public void getAllAnnotationsWithHandlerOk() {
        final List<Class> classes = AnnotationUtil.findAllAnnotatedClasses(RunWith.class, new AutoConfiguredHandler());

        Assertions.assertFalse(classes.isEmpty());
    }

    @DisplayName("map aggregates ok")
    @Test
    public void mapAggregatesOk() {
        final Map<String, Class<? extends AggregateBase>> classes = AnnotationUtil.mapAggregates(new AutoConfiguredHandler());

        //Recover personal aggregate
        Assertions.assertFalse(classes.isEmpty());
    }

}
