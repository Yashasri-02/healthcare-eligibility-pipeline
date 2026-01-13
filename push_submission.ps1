<#
Push submission helper

This script helps you push the current project to a GitHub repository.
It prefers the GitHub CLI (`gh`) if installed because it handles authentication nicely.

Usage (PowerShell):
    .\push_submission.ps1

It will:
 - Initialize a git repo if one doesn't exist
 - Stage all files
 - Create a commit
 - If `gh` is installed, offer to create a GitHub repo (private by default) and push
 - Otherwise prompt for a remote repo HTTPS URL and push

Note: Do NOT paste personal access tokens into this script. Use `gh auth login` or Git credential manager for auth.
#>

param()

function Run-Git([string]$args) {
    $git = "git"
    Write-Host "Running: git $args"
    & $git $args
    if ($LASTEXITCODE -ne 0) {
        throw "git failed: $args"
    }
}

try {
    if (-not (Test-Path ".git")) {
        Write-Host "No git repo found. Initializing..."
        Run-Git "init"
    } else {
        Write-Host "Git repo already initialized."
    }

    Run-Git "add -A"

    $commitMsg = Read-Host "Enter commit message (or press Enter for default)"
    if ([string]::IsNullOrWhiteSpace($commitMsg)) {
        $commitMsg = "Initial submission commit"
    }

    # If no commits yet, use --allow-empty to ensure a commit exists
    $hasCommit = (& git rev-parse --verify HEAD) 2>$null
    if ($LASTEXITCODE -ne 0) {
        Run-Git "commit -m `"$commitMsg`" --allow-empty"
    } else {
        Run-Git "commit -m `"$commitMsg`""
    }

    # Prefer GitHub CLI
    $gh = Get-Command gh -ErrorAction SilentlyContinue
    if ($gh) {
        Write-Host "GitHub CLI detected. You can create a repo and push via gh."
        $repoName = Read-Host "Enter repository name to create on GitHub (or press Enter to use current folder name)"
        if ([string]::IsNullOrWhiteSpace($repoName)) {
            $repoName = Split-Path -Leaf (Get-Location)
        }
        $privateChoice = Read-Host "Create repository as private? (Y/n)"
        if ($privateChoice -match '^[Nn]') { $private = $false } else { $private = $true }

        $visibility = if ($private) { "--private" } else { "--public" }
        Write-Host "Creating repo $repoName on GitHub ($($visibility.Trim()))..."
        gh repo create $repoName $visibility --source=. --remote=origin --push

        Write-Host "Remote 'origin' created and pushed via gh. Done."
        return
    }

    # Fallback: ask for remote URL
    $remote = git remote get-url origin 2>$null
    if ($LASTEXITCODE -eq 0 -and $remote) {
        Write-Host "Remote 'origin' already exists: $remote"
    } else {
        $remoteUrl = Read-Host "Enter remote repository HTTPS URL (e.g. https://github.com/you/repo.git)"
        if ([string]::IsNullOrWhiteSpace($remoteUrl)) {
            throw "No remote URL provided. Aborting."
        }
        Run-Git "remote add origin $remoteUrl"
    }

    # Ensure main branch
    Run-Git "branch -M main"

    Write-Host "Pushing to origin main..."
    Run-Git "push -u origin main"
    Write-Host "Push complete."
} catch {
    Write-Error "An error occurred: $_"
    exit 1
}
