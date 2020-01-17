package config;

public class Project {

    //Pour un hidoop fonctionnant seulement sur une machine
    //public static String nomMachine[] = {"localhost", "localhost", "localhost"};

    //Pour un hidoop fonctionnant sur plusieurs machines
    public static String nomMachine[] = {"localhost", "Griffon", "Pixie"};

    // des ports pour 5 machines
    public static int numPortHidoop[] = {4500, 4501, 4502, 4503, 4504, 4505};
    public static int numPortHDFS[] = {4100, 4101, 4102, 4103, 4104, 4105};

    // en l'état, fonctionne pour 2 machines
    public static int nbMachine = 2 ;



}