package ru.ruslan;

import java.util.HashSet;
import java.util.Set;

public class Main {
    public static void main(String[] args) {

        System.out.println("Hello world!");

        Set<Runnable> sourceList = new HashSet<>();
        IClass item = new IClass();

        sourceList.add(item::exec);
        sourceList.add(null);
        sourceList.add(item::exec);
        sourceList.add(item::exec);

        for( Runnable element: sourceList ) {
            if (element != null) {
                element.run();
                sourceList.remove(element);
            }
        }

        System.out.println( sourceList.size() );
    }
}