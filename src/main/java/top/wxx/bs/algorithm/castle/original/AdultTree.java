package top.wxx.bs.algorithm.castle.original;

import top.wxx.bs.TreeStructure.Tree;

public class AdultTree {

    public static Tree WorkClass;
    public static Tree Education;
    public static Tree MaritalStatus;
    public static Tree Race;
    public static Tree Gender;

    static {

        //Work-class = Private, Self-emp-not-inc, Self-emp-inc, Federal-gov, Local-gov, State-gov, Without-pay, Never-worked
        WorkClass = new Tree();
        WorkClass.addNode("Work-class");
        WorkClass.addNode("Private", "Work-class");
        WorkClass.addNode("Self-emp-not-inc", "Work-class");
        WorkClass.addNode("Self-emp-inc", "Work-class");
        WorkClass.addNode("Federal-gov", "Work-class");
        WorkClass.addNode("Local-gov", "Work-class");
        WorkClass.addNode("State-gov", "Work-class");
        WorkClass.addNode("Without-pay", "Work-class");
        WorkClass.addNode("Never-worked", "Work-class");


        /**
         *
         Compulsory-Education=1st-4th,Middle-School,High-School
         Middle-School = 5th-6th , 7th-8th
         High-School = 9th, 10th, 11th, 12th

         Higher-Education = Pre-University, College, University
         Pre-University = HS-grad, Prof-school,
         College = Some-college ,Assoc-acdm, Assoc-voc
         University = Bachelors , GraduateSchool
         GraduateSchool = Masters,Doctorate
         */
        Education = new Tree();
        //Education = Preschool,Compulsory-Education,Higher-Education
        Education.addNode("Education");
        Education.addNode("Preschool", "Education");
        Education.addNode("Compulsory-Education", "Education");
        Education.addNode("Higher-Education", "Education");

        /**
         *  Compulsory-Education=1st-4th,Middle-School,High-School
         Middle-School = 5th-6th , 7th-8th
         High-School = 9th, 10th, 11th, 12th
         */

        Education.addNode("1st-4th", "Compulsory-Education");
        Education.addNode("Middle-School", "Compulsory-Education");
        Education.addNode("High-School", "Compulsory-Education");

        Education.addNode("5th-6th", "Middle-School");
        Education.addNode("7th-8th", "Middle-School");

        Education.addNode("9th", "High-School");
        Education.addNode("10th", "High-School");
        Education.addNode("11th", "High-School");
        Education.addNode("12th", "High-School");


        /**
         * Higher-Education = Pre-University, College, University
         Pre-University = HS-grad, Prof-school,
         College = Some-college ,Assoc-acdm, Assoc-voc
         University = Bachelors , GraduateSchool
         GraduateSchool = Masters,Doctorate
         */

        Education.addNode("Pre-University", "Higher-Education");
        Education.addNode("College", "Higher-Education");
        Education.addNode("University", "Higher-Education");

        Education.addNode("HS-grad", "Pre-University");
        Education.addNode("Prof-school", "Pre-University");

        Education.addNode("Some-college", "College");
        Education.addNode("Assoc-acdm", "College");
        Education.addNode("Assoc-voc", "College");

        Education.addNode("Bachelors", "University");
        Education.addNode("GraduateSchool", "University");
        Education.addNode("Masters", "GraduateSchool");
        Education.addNode("Doctorate", "GraduateSchool");
        /**
         * Marital-status = Married, Not-married
         * Married = Married-civ-spouse, Married-spouse-absent, Married-AF-spouse
         * Not-married = Divorced, Never-married, Separated, Widowed
         *
         */
        MaritalStatus = new Tree();
        MaritalStatus.addNode("Marital-status");
        MaritalStatus.addNode("Married", "Marital-status");
        MaritalStatus.addNode("Not-married", "Marital-status");

        MaritalStatus.addNode("Married-civ-spouse", "Married");
        MaritalStatus.addNode("Married-spouse-absent", "Married");
        MaritalStatus.addNode("Married-AF-spouse", "Married");

        MaritalStatus.addNode("Divorced", "Not-married");
        MaritalStatus.addNode("Never-married", "Not-married");
        MaritalStatus.addNode("Separated", "Not-married");
        MaritalStatus.addNode("Widowed", "Not-married");

        // Race = White, Asian-Pac-Islander, Amer-Indian-Eskimo, Black, Other
        Race = new Tree();
        Race.addNode("Race");

        Race.addNode("White", "Race");
        Race.addNode("Asian-Pac-Islander", "Race");
        Race.addNode("Amer-Indian-Eskimo", "Race");
        Race.addNode("Black", "Race");
        Race.addNode("Other", "Race");

        //Gender = Male Female
        Gender = new Tree();
        Gender.addNode("Gender");
        Gender.addNode("Male", "Gender");
        Gender.addNode("Female", "Gender");
    }

}
