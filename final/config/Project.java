package config;

public class Project {

    //Pour un hidoop fonctionnant seulement sur une machine
   //public static String nomMachine[] = {"localhost", "localhost", "localhost"};

    //Pour un hidoop fonctionnant sur plusieurs machines
    public static String nomMachine[] = {"localhost", "Griffon", "Pixie"};
    public static String nomMachine1[] = {"localhost", "localhost", "localhost"};

    // des ports pour 5 machines
    public static int numPortHidoop[] = {4000, 4001, 4002, 4003, 4004, 4005};
    public static int numPortHDFS[] = {4000, 4101, 4102, 4103, 4104, 4105};

    // en l'état, fonctionne pour 2 machines
    public static int nbMachine = 2 ;

    public static String PATH = "~/Documents/Annee_2/hidoop/final";



}