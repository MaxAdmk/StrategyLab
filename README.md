# StrategyLab
kcat -b localhost:9092 -t air_quality -C -o beginning -e
This command in bash to see all the messages that we sent to kafka
For redis:
docker exec -it lab4_v1_patterns-redis-1 redis-cli
then: KEYS *

To clear redis keyss:
docker exec -it lab4_v1_patterns-redis-1 redis-cli FLUSHDB

