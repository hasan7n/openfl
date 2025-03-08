pkill -f "python collaborator.py *"
NUM_COLS=3
a=0
rm -rf ~/testws/analysis
mkdir ~/testws/analysis
rm -rf ~/testws/analysis2
mkdir ~/testws/analysis2

rm -rf logs/*
while [ $a -lt $NUM_COLS ]; do
    a=$(expr $a + 1)
    python collaborator.py $a >logs/col$a.log 2>&1 &
done
