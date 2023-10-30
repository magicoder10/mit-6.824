cd ./raft || exit
rm -rf logs/running && ../dstest --workers 8 --output logs/running --race --timeout 60s --iter 8 --archive TestFigure8Unreliable2C

cd ../ || exit
for i in {0..7}
do
    go run tool/*.go stats -o raft/logs/analyze/stats-new-$i.txt -i raft/logs/running/TestFigure8Unreliable2C_$i.log
done