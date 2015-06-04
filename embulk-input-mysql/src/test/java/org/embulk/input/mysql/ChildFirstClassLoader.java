package org.embulk.input.mysql;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

public class ChildFirstClassLoader
        extends URLClassLoader
{
    public ChildFirstClassLoader(List<URL> urls, ClassLoader parent)
    {
        super(urls.toArray(new URL[urls.size()]), parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve)
            throws ClassNotFoundException
    {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loadedClass = findLoadedClass(name);
            if (loadedClass != null) {
                return resolveClass(loadedClass, resolve);
            }

            try {
                return resolveClass(findClass(name), resolve);
            } catch (ClassNotFoundException ignored) {
            }

            return super.loadClass(name, resolve);
        }
    }

    private Class<?> resolveClass(Class<?> clazz, boolean resolve)
    {
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

}
