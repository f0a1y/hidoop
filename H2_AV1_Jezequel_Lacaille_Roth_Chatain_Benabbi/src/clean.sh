ssh $USER@griffon 'pkill -f java.*DaemonHDFS*'
ssh $USER@acdc 'pkill -f java.*DaemonHDFS*'
ssh $USER@pinkfloyd 'pkill -f java.*DaemonHDFS*'
ssh $USER@manticore 'pkill -f java.*DaemonHDFS*'

ssh $USER@griffon 'pkill -f java.*DaemonImpl*'
ssh $USER@acdc 'pkill -f java.*DaemonImpl*'
ssh $USER@pinkfloyd 'pkill -f java.*DaemonImpl*'
ssh $USER@manticore 'pkill -f java.*DaemonImpl*'
