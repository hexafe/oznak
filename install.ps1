# install.ps1
# Bootstrap script for oznak app

$pythonInstaller = "https://www.python.org/ftp/python/3.12.6/python-3.12.6-amd64.exe"
$installerPath   = "$env:TEMP\python-installer.exe"
$repoUrl         = "https://github.com/hexafe/oznak.git"
$repoName        = "oznak"
$venvName        = "venv"
$requirements    = "requirements.txt"

Write-Host "=== Checking Python installation ==="

# Check if python or py is available
$python = Get-Command python -ErrorAction SilentlyContinue
$py     = Get-Command py -ErrorAction SilentlyContinue

if (-not $python -and -not $py) {
    Write-Host "Python not found. Downloading..."
    Invoke-WebRequest $pythonInstaller -OutFile $installerPath
    Write-Host "Installing Python..."
    Start-Process -FilePath $installerPath -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1" -Wait
    Write-Host "Python installed successfully."
} else {
    Write-Host "Python is already installed."
}

# Prefer py launcher if available
if ($py) {
    $pythonCmd = "py"
} else {
    $pythonCmd = "python"
}

# Check if git is available
$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    Write-Error "Git is not installed! Please install Git and re-run the script."
    exit 1
}

# Clone repo if not exists
if (-not (Test-Path $repoName)) {
    Write-Host "=== Cloning repository $repoUrl ==="
    git clone $repoUrl
} else {
    Write-Host "Repository already exists. Pulling latest changes..."
    Set-Location $repoName
    git pull
    Set-Location ..
}

# Move into repo directory
Set-Location $repoName

# Create venv if not exists
if (-not (Test-Path $venvName)) {
    Write-Host "=== Creating virtual environment ==="
    & $pythonCmd -m venv $venvName
} else {
    Write-Host "Virtual environment already exists."
}

# Activate venv in current shell
Write-Host "=== Activating virtual environment ==="
. ".\$venvName\Scripts\Activate.ps1"

# Install requirements
if (Test-Path $requirements) {
    Write-Host "=== Installing requirements from $requirements ==="
    pip install --upgrade pip
    pip install -r $requirements
} else {
    Write-Host "No requirements.txt found, skipping package installation."
}

Write-Host "=== Setup complete! Virtual environment is active inside oznak repo ==="
