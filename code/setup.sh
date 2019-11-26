#sudo apt-get update
pip install --user hdfs, pika, pandas
nohup jupyter-notebook &
hdfs dfs -put /home/gcpkey/data.csv /hadoop
