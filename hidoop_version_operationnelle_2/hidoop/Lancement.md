Avant de commencer l'exécution des processus, il faut regarder si la configuration (dans le fichier src/config/Project.java) est correct. S'il y a des changements à faire, il faut alors recompiler le fichier et mettre le fichier executable .class dans le dossier bin/config/Project.class.

On peut ensuite démarrer les terminaux depuis le dossier bin/.

Pour lancer les daemons HDFS : 
	- java hdfs.daemon.DaemonHDFS <identifiant>

Pour lancer les daemons Hidoop :
	- java ordo.DaemonImpl <identifiant>

dans les deux cas, identifiant doit être compris entre 1 et la taille des tableaux du fichier src/config/Project.java. 
Pour chaque daemon Hidoop, il faut un daemon HDFS avec le même identifiant.
Il doit y avoir autant de couple de daemons Hidoop et HDFS lancés que défini dans le fichier src/config/Project.java.

Pour lancer le serveur HDFS :
	- java hdfs.serveur.ServerHDFS

Une fois les daemons Hidoop et HDFS et le serveur HDFS lancés, on peut lancer le client HDFS et le serveur Hidoop (les deux se termine une fois leur programme exécuté, contrairement aux autres processus).

Pour lancer le client HDFS :
	- java hdfs.Client <commande> <nomFichier>

La commande est un entier :
	- 1 : upload de fichier
	- 2 : download de fichier
	- 3 : suppression de fichier

Une fois qu'un fichier a été envoyé sur le serveur HDFS, les fragments vont être répartis sur les noeuds du cluster. On peut alors lancer le serveur Hidoop qui va exécuter un traitement particulier sur ces fragments.

Pour lancer le serveur Hidoop :
	- java map.MapReduceImpl <nomFichier>
	- java application.MyMapReduce <nomFichier>

Le premier serveur sert à tester que les connexions entre serveurs et daemons fonctionnent.
Le deuxième serveur va compter le nombre d'occurrence des mots d'un fichier.

modifier sendAll de Cluster
modifier envoi des fragment pour taille élevé
opération de vérification des fragments des noeuds
fichier absolu
