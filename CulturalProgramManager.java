import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

class Student {
    String name;
    int culturalProgramMarks;

    public Student(String name, int culturalProgramMarks) {
        this.name = name;
        this.culturalProgramMarks = culturalProgramMarks;
    }

    public String getName() {
        return name;
    }

    public int getCulturalProgramMarks() {
        return culturalProgramMarks;
    }

    @Override
    public String toString() {
        return name + " - Marks: " + culturalProgramMarks;
    }
}

public class CulturalProgramManager {
    public static void main(String[] args) {
        // Create an ArrayList to store students
        ArrayList<Student> studentList = new ArrayList<>();

        // Add students who participated in cultural programs
        studentList.add(new Student("Alice", 95));
        studentList.add(new Student("Bob", 88));
        studentList.add(new Student("Charlie", 72));
        studentList.add(new Student("David", 94));
        studentList.add(new Student("Eve", 85));

        // Search for a student who participated in a cultural program
        String searchName = "David";
        for (Student student : studentList) {
            if (student.getName().equals(searchName)) {
                System.out.println("Found: " + student);
            }
        }

        // Sort the students by cultural program marks in descending order
        Collections.sort(studentList, new Comparator<Student>() {
            @Override
            public int compare(Student s1, Student s2) {
                return Integer.compare(s2.getCulturalProgramMarks(), s1.getCulturalProgramMarks());
            }
        });

        // Print the sorted list
        System.out.println("Students sorted by marks in cultural programs:");
        for (Student student : studentList) {
            System.out.println(student);
        }
    }
}
