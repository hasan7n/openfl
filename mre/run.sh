pkill -f "python collaborator.py *"
NUM_COLS=50
rm -rf analysis
mkdir analysis
rm -rf analysis2
mkdir analysis2
rm -rf logs/*
a=0
while [ $a -lt $NUM_COLS ]; do
    a=$(expr $a + 1)
    python collaborator.py $a >logs/col$a.log 2>&1 &
done
