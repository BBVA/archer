package com.bbva.dataprocessors.util;

import com.bbva.dataprocessors.util.beans.Person;
import com.bbva.dataprocessors.util.beans.PersonNoDefaultConstructor;
import com.bbva.dataprocessors.util.beans.PersonWithoutGetters;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class ObjectUtilsTest {

    @DisplayName("Check merge ok")
    @Test
    public void mergeObjectOk() {

        final Person personLast = new Person("name", "last name");
        final Person personPhone = new Person();
        personPhone.setPhone("123456789");

        final Person person = (Person) ObjectUtils.merge(personLast, personPhone);

        Assertions.assertAll("mergedObject",
                () -> Assertions.assertEquals("name", person.getName()),
                () -> Assertions.assertEquals("last name", person.getLastName()),
                () -> Assertions.assertEquals("123456789", person.getPhone())
        );
    }


    @DisplayName("Bean without default constructor returns the last value")
    @Test
    public void mergeFailReturnLast() {

        final PersonNoDefaultConstructor personLast = new PersonNoDefaultConstructor("name", "last name");
        final PersonNoDefaultConstructor personPhone = new PersonNoDefaultConstructor("123456789");

        final PersonNoDefaultConstructor person = (PersonNoDefaultConstructor) ObjectUtils.merge(personLast, personPhone);

        Assertions.assertAll("noMergedObject",
                () -> Assertions.assertEquals("name", person.getName()),
                () -> Assertions.assertEquals("last name", person.getLastName()),
                () -> Assertions.assertEquals(null, person.getPhone())
        );
    }

    @DisplayName("Bean without getters/setters returns nulls")
    @Test
    public void mergeWithoutGetters() {

        final PersonWithoutGetters personLast = new PersonWithoutGetters("name", "last name");
        final PersonWithoutGetters personPhone = new PersonWithoutGetters("123456789");

        final PersonWithoutGetters person = (PersonWithoutGetters) ObjectUtils.merge(personLast, personPhone);

        Assertions.assertAll("noMergedObject",
                () -> Assertions.assertNotNull(person)
        );
    }

    @DisplayName("Get merged value")
    @Test
    public void getMergedValue() {

        final String nullField = ObjectUtils.getNewMergedValue(null, null);
        final String field1 = ObjectUtils.getNewMergedValue("field1", null);
        final String field2 = ObjectUtils.getNewMergedValue(null, "field2");
        final String merged = ObjectUtils.getNewMergedValue(field1, field2);

        Assertions.assertAll("getMergedValue",
                () -> Assertions.assertEquals(null, field1),
                () -> Assertions.assertEquals("field2", field2),
                () -> Assertions.assertEquals("field2", merged),
                () -> Assertions.assertEquals(null, nullField)
        );
    }


    @DisplayName("Get field getter and setter")
    @Test
    public void getFieldMethod() {

        final String getter = ObjectUtils.getFieldNameMethod("field", true);
        final String setter = ObjectUtils.getFieldNameMethod("field", false);

        Assertions.assertAll("getFieldMethod",
                () -> Assertions.assertEquals("getField", getter),
                () -> Assertions.assertEquals("setField", setter)
        );
    }
}
