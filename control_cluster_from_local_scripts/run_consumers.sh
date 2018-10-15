for worker in $(seq 2 9); do
    peg sshcmd-node kfc $worker "python3 /home/ubuntu/eye_of_sauron/run_consumers.py" &
    echo "[kfc $worker] Started consuming.."
    sleep 1s
done
