#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Usage: $0 <PROJECT_ID> <SERVICE_ACCOUNT_NAME>"
    exit 1
fi

PROJECT_ID="$1"
SERVICE_ACCOUNT_NAME="$2"

check_and_install_gcloud() {
    if command -v gcloud &>/dev/null; then
        echo "Google Cloud CLI is already installed."
    else
        read -p "Google Cloud CLI is not installed. Install now? (y/n) " yn
        case $yn in
            [Yy]*) 
            install_gcloud
            create_service_account
            check_and_setup_app_default_login
            ;;
            *) echo "Skipping Google Cloud CLI installation.";;
        esac
    fi
}

install_gcloud() {
    echo "Installing Google Cloud CLI"
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add -
    sudo apt-get update && sudo apt-get install google-cloud-sdk -y
}

check_and_setup_app_default_login() {
    if ! gcloud auth application-default print-access-token &>/dev/null; then
        echo "Application Default Credentials are not set up. Setting up now..."
        gcloud auth application-default login
    else
        echo "Application Default Credentials are already set up."
    fi
}

create_service_account() {
    echo "Creating a new service account..."
    if ! command -v gcloud &>/dev/null; then
        read -p "Google Cloud CLI is not installed. Install now? (y/n) " yn
        if [[ "$yn" == [Yy]* ]]; then
            install_gcloud
            if ! command -v gcloud &>/dev/null; then
                echo "Failed to install Google Cloud CLI. Exiting."
                exit 1
            fi
        else
            echo "Skipping Google Cloud CLI installation. Cannot proceed without Google Cloud CLI."
            exit 1
        fi
    fi

    if gcloud iam service-accounts list --format="value(email)" --project="$PROJECT_ID" | grep -q "${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"; then
        echo "Service account $SERVICE_ACCOUNT_NAME already exists."
    else
        gcloud iam service-accounts create "$SERVICE_ACCOUNT_NAME" --project="$PROJECT_ID"
        gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com" --role="roles/owner"
        gcloud iam service-accounts keys create "${HOME}/${SERVICE_ACCOUNT_NAME}-key.json" --iam-account="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
        echo "Service account $SERVICE_ACCOUNT_NAME created and key stored at ${HOME}/${SERVICE_ACCOUNT_NAME}-key.json"
    fi
}


export_tf_variables() {
    export TF_VAR_project_id="${PROJECT_ID}"
    export TF_VAR_credentials_file_path="${HOME}/${SERVICE_ACCOUNT_NAME}-key.json"
    export TF_VAR_service_account_email="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
}

check_and_install_terraform() {
    if command -v terraform &>/dev/null; then
        echo "Terraform is already installed."
        terraform --version
    else
        read -p "Terraform is not installed. Install now? (y/n) " yn
        if [[ "$yn" == [Yy]* ]]; then
            install_terraform
        else
            echo "Skipping Terraform installation."
        fi  
    fi 
}


install_terraform() {
    echo "Installing Terraform..."
    sudo apt-get update && sudo apt-get install -y gnupg software-properties-common
    wget -O- https://apt.releases.hashicorp.com/gpg | \
    gpg --dearmor | \
    sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg > /dev/null
    gpg --no-default-keyring \
    --keyring /usr/share/keyrings/hashicorp-archive-keyring.gpg \
    --fingerprint
    echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
    https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
    sudo tee /etc/apt/sources.list.d/hashicorp.list
    sudo apt-get update && sudo apt-get install terraform

    if terraform --version >/dev/null 2>&1; then
        echo "Terraform installed successfully."
        terraform --version
    else
        echo "Failed to install Terraform."
        exit 1
    fi
}

destroy_resources() {
    read -p "Do you want to destroy existing resources?: (y/n) " yn
    if [[ "$yn" == [Yy]* ]]; then
        echo "Destroying existing terraform resources"
        terraform destroy
    else
        echo "Skipping destroying existing resources"
    fi
}

install_python() {
    echo "Installing Python 3.8..."
    sudo apt-get update && sudo apt-get install python3.8 -y
    sudo apt-get install python3-pip
    sudo apt-get install build-essential libssl-dev libffi-dev python-dev
    alias python=python3
    alias pip3=pip
}


install_virtualenv() {
    echo "Installing virtual environment..."
    sudo apt install virtualenv
}


create_virtualenv() {
    echo "Creating a new virtual environment..."
    cd $HOME
    virtualenv .venv -p python3.8
    alias activate_venv="source ~/.venv/bin/activate"
    echo "subpath from root: .venv"
}


check_and_setup_python_env() {
    read -p "Is Python and virtual environment set up? (y/n) " yn
    case $yn in
        [Yy]*)
            read -p "Enter the subpath from root to your virtual environment: " venv_path
            . ~/$venv_path/bin/activate
            pip list
            ;;
        *)
            install_python
            install_virtualenv
            create_virtualenv
            ;;
    esac
}


check_and_install_dependencies() {
    read -p "Are all Python dependencies installed? (y/n) " yn
    if [[ "$yn" == [Nn]* ]]; then
        read -p "Enter the path to your dependencies file: " deps_file
        pip install -r "$deps_file"
    else
        echo "All dependencies are already satisfied."
    fi
}

create_infra() {
    python utils/create_infra.py -o create -p $PROJECT_ID
}


run_terraform() {
    echo "Setting environment variables for Terraform"

    # read -p "Enter the path to your service account credentials file: " credentials_file

    echo "Initializing and applying Terraform configuration..."
    terraform init
    terraform plan
    terraform apply -auto-approve
}


check_and_install_gcloud
check_and_setup_app_default_login
create_service_account
check_and_setup_python_env
check_and_install_dependencies
create_infra
check_and_install_terraform
export_tf_variables
destroy_resources
run_terraform
