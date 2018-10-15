# just a hack, it'll kill all python3 process, don't use if the server is running other important python3 processes.
for worker in $(seq 1 9); do
    peg sshcmd-node kfc $worker "pkill python3"
    echo "Killed all python3 processes on kfc $worker"
done