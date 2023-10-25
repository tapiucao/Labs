##install python packages
conda install -c conda-forge kaggle
conda install -c conda-forge pyspark
conda install -c conda-forge azure-storage-blob
conda install -c conda-forge azure-storage-file-datalake
sudo apt install default-jre --java for spark
conda install -c conda-forge azure-keyvault-secrets
conda install -c conda-forge microsoft azure-monitor-ingestion
conda install -c conda-forge loguru

##install GIT
sudo apt install git-all
git remote add origin https://github.com/tapiucao/Labs.git

##install azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
sudo apt-get update
sudo apt-get install ca-certificates curl apt-transport-https lsb-release gnupg
sudo mkdir -p /etc/apt/keyrings
curl -sLS https://packages.microsoft.com/keys/microsoft.asc |
    gpg --dearmor |
    sudo tee /etc/apt/keyrings/microsoft.gpg > /dev/null
sudo chmod go+r /etc/apt/keyrings/microsoft.gpg
AZ_DIST=$(lsb_release -cs)
echo "deb [arch=`dpkg --print-architecture` signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/repos/azure-cli/ $AZ_DIST main" |
    sudo tee /etc/apt/sources.list.d/azure-cli.listdo

docker:

docker build -t meu-airflow .
docker run --rm -d -p 8080:8080 -e LOAD_EX=n -e AIRFLOW__CORE__EXECUTOR=SequentialExecutor meu-airflow airflow webserver
