for worker in $(seq 2 9); do
    peg sshcmd-node kfc $worker "pkill python3"
    echo "Killed all python3 processes on kfc $worker"
done