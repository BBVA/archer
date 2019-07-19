/*
    This file is part of "nukkit xWorlds test tools".

    "nukkit xWorlds test tools" is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    "nukkit xWorlds test tools" is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with "nukkit xWorlds test tools". If not, see <http://www.gnu.org/licenses/>.

 */
package com.bbva.common.util;

import java.util.HashSet;
import java.util.Set;

public class PowermockState {

    private ClassLoader origClassLoader;

    private final Set<Class<?>> classesToPrepare = new HashSet<>();

    private final Set<String> packagesToIgnore = new HashSet<>();

    public ClassLoader getOrigClassLoader() {
        return origClassLoader;
    }

    void setOrigClassLoader(final ClassLoader origClassLoader) {
        this.origClassLoader = origClassLoader;
    }

    public String[] getClassesToPrepareAsString() {
        final String[] result = new String[classesToPrepare.size()];
        final Class[] classes = classesToPrepare.toArray(new Class[classesToPrepare.size()]);
        for (int i = 0; i < result.length; i++) {
            result[i] = classes[i].getName();
        }
        return result;
    }

    public String[] getPackagesToIgnoreAsArray() {
        final String[] result = packagesToIgnore.toArray(new String[packagesToIgnore.size()]);
        return result;
    }

}
