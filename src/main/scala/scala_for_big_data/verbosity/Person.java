package scala_for_big_data.verbosity;

import java.util.Objects;

public class Person {

    private final String name;
    private final double age;

    public Person (String name, double providedAge) {
        this.name = name;
        this.age = providedAge;
    }

    @Override
    public int hashCode  () {
        int hash = 10;
        hash = 23 * hash + Objects.hashCode(this.name);
        return hash;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final Person other = (Person) obj;

        if (!Objects.equals(this.name, other.name)) {
            return false;
        }
        if (Double.doubleToLongBits(this.age) != Double.doubleToLongBits(other.age)) {
            return false;
        }
        return true;
    }
    @Override
    public String toString() {
        return "Test{" + "name=" + name + ", age=" + age + '}';
    }

    public static void main(String[] args) {

        Person person_0 = new Person("Artur",33);
        Person person_1 = new Person("Artur",33);
        System.out.println(person_0.equals(person_1));
    }
}
