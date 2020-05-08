javac */*.java */*/*.java


mate-terminal --window -e "/bin/bash -c \"java hdfs.server.ServerHDFS; exec /bin/bash\"" \
                --tab -e "/bin/bash -c \"java hdfs.daemon.DaemonHDFS 0; exec /bin/bash\"" \
                --tab -e "/bin/bash -c \"java hdfs.daemon.DaemonHDFS 1; exec /bin/bash\"" \
                --tab -e "/bin/bash -c \"java hdfs.daemon.DaemonHDFS 2; exec /bin/bash\""

mate-terminal --window -e "/bin/bash -c \"java ordo.DaemonImpl 0; exec /bin/bash\"" \
                --tab -e "/bin/bash -c \"java ordo.DaemonImpl 1; exec /bin/bash\"" \
                --tab -e "/bin/bash -c \"java ordo.DaemonImpl 2; exec /bin/bash\""


printf '\n java application/MyMapReduce filesample.txt \n'
printf '\n \n java hdfs.HdfsClient write line filesample.txt'
printf '\n java hdfs.HdfsClient read filesample.txt'
printf '\n java hdfs.HdfsClient delete filesample.txt \n'
