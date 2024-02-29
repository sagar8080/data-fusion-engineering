# Setting Up WSL and Ubuntu 20.04 on Windows

## 1. Install WSL and Ubuntu 20.04 on a Windows Machine

### Step 1: Enable WSL Feature

- Open PowerShell as Administrator.
- Run the following command:

```
wsl --install

dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart

dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

wsl --set-default-version 2
```
- For a more detailed answer you can follow this [!LINK](https://learn.microsoft.com/en-us/windows/wsl/install)

### Step 2: Install Ubuntu 20.04 from Microsoft Store
- Open Microsoft Store.
- Search for "Ubuntu 20.04".
- Click "Install" and follow the on-screen instructions.

### Step 3: Set Up Ubuntu 20.04
- Launch Ubuntu 20.04 from the Start menu.
- Follow the prompts to set up a new user account and password.

## 2. Install Python 3.10 on Ubuntu

### Step 1: Update Package Lists
- Open a terminal in Ubuntu.
- Run the following command to update the package lists: `sudo apt update`

### Step 2: Install Python 3.10 Dependencies
- Run the following command to install prerequisites: `sudo apt install software-properties-common`


### Step 3: Add DeadSnakes PPA
- Run the following commands to add the DeadSnakes PPA, which provides newer Python versions:
```bash
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
```


### Step 4: Install Python 3.10
- Run the following command to install Python 3.10: `sudo apt install python3.10`


## 3. Install Git

### Step 1: Update Package Lists
- Open a terminal in Ubuntu.
- Run the following command to update the package lists: `sudo apt install git`


### Step 3: Verify Installation
- After installation, verify that Git is installed by running: `git --version`





