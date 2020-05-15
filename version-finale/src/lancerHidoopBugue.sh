javac -encoding ISO-8859-1 */*.java 
javac -encoding ISO-8859-1 */*/*.java 

no_max_daemon=5;
if [ "$#" -eq 1 ]; then
	no_max_daemon=$(($1-1));
fi

# Lancement du serveur et des N démons
commande="mate-terminal --window -e \"/bin/bash -c \\\"java hdfs.server.ServerHDFS; exec /bin/bash \\\"\" \ "
for i in `seq 0 $no_max_daemon`;
do
	# commande=$commande"\n --tab -e \"/bin/bash -c \\\"java hdfs.daemon.DaemonHDFS $i; exec /bin/bash\\\"\" \ "
a=3
done
echo $commande
$($commande)

# Lancement des N DaemonImpl
#gnome-terminal --window -e "/bin/bash -c \"java ordo.DaemonImpl 0; exec /bin/bash\"" \
#for i in `seq 1 $no_max_daemon`
#do
#	--tab -e "/bin/bash -c \"java ordo.DaemonImpl $i; exec /bin/bash\"" \
#done



printf '\n java application/MyMapReduce filesample.txt \n'
printf '\n \n java hdfs.HdfsClient write line filesample.txt'
printf '\n java hdfs.HdfsClient read filesample.txt'
printf '\n java hdfs.HdfsClient delete filesample.txt \n'
