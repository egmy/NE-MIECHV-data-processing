
# Setup for NE MIECHV Coding  

- Project: Python project for coding the Nebraska MIECHV data sourcing process.
- Purpose of this document: Set up local computer with all initial installations, settings, & setup needed to run project.

- Summary: 
    - Install & set up Visual Studio Code, Git, and Conda. 
    - Create Conda & Pipenv virtual environments, through which we will install Python & Python packages.
    - Instructions and advice for using these software.

- Locations of this document (should be kept up-to-date & identical in both):
    - Shared Drive: U:/Working/nehv_ds_code_repository/_INSTRUCTIONS_for_local_setup.md 
    - Local code repository (after created): H:/git/nehv_ds_code_repository/_INSTRUCTIONS_for_local_setup.md 

- Version: 15 

________________________________________________________________________________

# NE MIECHV project notes 

Miro board: https://miro.com/app/board/uXjVPRUZL6Y=/

________________________________________________________________________________

# 1. Enter Nebraska's portal & Open Instructions 

1. Sign into Nebraska's virtual workspace through the VMware Horizon Client.
    - Note: If the screen locks & you're asked to press Ctrl+Alt+Del, you can use the button for "Ctrl+Alt+Del" in the upper right of the VMware Horizon Client window.

2. Assure you can open the Shared "U" Drive in File Explorer.
    - If not, then:
        1.	In the NEHV virtual workspace >> Open File Explorer.
        2.	Go to "This PC" >> (at top) "Computer" >> "Map network drive".
        3.	Settings:
            1.	Drive: "U"
            2.	Folder: \\fs1.hhss.local\familywise$   
            3.	SELECT "Reconnect at sign-in"
        4.	"Finish"

3. Open these instructions with Notepad: 
    - U:/Working/nebraska_miechv_coded_data_source/_INSTRUCTIONS_for_local_setup.md

4. In Notepad, turn on Word Wrap (if it's not already):
    - Tab "Format" >> "Word Wrap".

________________________________________________________________________________

# 2. Adjust Windows Computer Settings 

- Adjust computer settings to see all file extensions:
    - Purpose: To be able to edit file extensions (which we will do), you need to be able to see them.
    - On a Windows machine:
        1. Open a File Explorer to "This PC".
        2. Click "View" tab at top >> Click "Options" on right.
        3. In pop-up "Folder Options" >> Click "View" tab at top.
        4. UN-select: "Hide extensions for known file types"
        - Tip (Optional):
            - Select: "Display the full path in the title bar"
            - Select: "Show hidden file, folders, or drives"

________________________________________________________________________________

# 3. Install VS Code

- Visual Studio Code (VS Code) is a source code editor we will use to write code.
    - Install VS Code BEFORE Git because a setting during Git setup asks for VS Code (we will finish VS Code setup after setting up Git).

## Check if VS Code already installed (if so REMOVE):
    1. Go to Windows Settings (click Windows icon in bottom left >> click Gear icon that pops up above it).
    2. In Settings >> go to "Apps"
    3. In Apps >> in section "Apps & features" >> Check if "Microsoft Visual Studio Code" is in list.
    4. If it is, try to uninstall. 
    5. If uninstall fails, contact the Nebraska software team to have them remove it.
    6. Once your machine has no VS Code, install it yourself.
    - Why not use their version? 
        - (a) There are some settings that can only be changed during setup. 
        - (b) Two versions of VS Code on a computer can cause errors.

## Install VS Code before Git:
    1. Download (most recent version): https://code.visualstudio.com/download 
        1. Click blue "Windows" button.
    2. Run Installation.
    3. When installing, use these settings:
        1. Default Location.
        2. Default Shortcuts.
        3. "Select Additional Tasks" Page >> Under "Other":
            1. SELECT "Add 'Open with Code' action to Windows Explorer file context menu"
            2. SELECT "Add 'Open with Code' action to Windows Explorer directory context menu"
            3. SELECT "Register Code as an editor for supported file types"
            4. SELECT "Add to PATH (requires shell restart)"

- After VS Code installation:
    - You can close this instruction file & re-open in VS Code:
        1. Find the file here: U:/Working/nehv_ds_code_repository/_INSTRUCTIONS_for_local_setup.md
        2. Just double click to open (and it should open in VS Code).
        3. Open the file Outline to jump between sections faster (you can click in the outline to move in the document):
            1. In the upper-left, click the "Explorer" icon (image of files/papers).
            2. In the new panel, toward the bottom, click "Outline".
            3. Grab the line just above "Outline" and drag up to expand.

________________________________________________________________________________

# 4. Git 

- Git is a version control system we will use to track changes to our code & enable multiple people to work on the code at the same time.
    - Git install includes a Git Bash prompt (a command-line interface shell).
    - Resources:
        - Documentation: https://git-scm.com/doc
        - See also: https://www.git-tower.com/blog/git-bash/ 

________________________________________________________________________________

## 4.1. Install Git 

- Install Git AFTER Visual Studio Code (because during Git install we will set VS Code as Git's default editor).

1. Download (most recent version): https://git-scm.com/ 
2. Run install .exe.
3. Enter password if asked if ok to install.
4. When installing, use these settings:
    1. Default location.
    2. "Select Components": 
        - SELECT "Check daily for Git for Windows updates"
        - and other Defaults, for example:
            - SELECT "Open Git Bash here"
            - SELECT "Associate .git..." & "Associate .sh..."
    3. Default start menu folder (yes create).
    4. "Choosing the default editor used by Git":
        - SELECT "Use Visual Studio Code as Git's default editor"
    5. "Adjusting the name of the initial branch in new repositories":
        - SELECT "Override the default branch name for new repositories": main 
    6. Default PATH environment (Recommended "Git from the command line and also from 3rd-party software").
    7. Default SSH executable ("Use bundled OpenSSH").
    8. Default HTTPS transport backend ("Use the OpenSSL library").
    9. "Configuring the line ending conversions": 
        - SELECT "Checkout as-is, commit Unix-style line endings"
    10. Default terminal emulator to use with Git Bash ("Use MinTTY").
    11. Default behavior of git pull ("fast-forward or merge").
    12. Default credential helper ("Git Credential Manager").
            - Aside: Information about Git Credential Manager Core: 
                https://github.com/microsoft/Git-Credential-Manager-Core/blob/main/docs/faq.md#about-the-project
    13. Default extra options (Only "Enable file system caching").
    14. Default experimental options (NONE selected).
    15. Install!

________________________________________________________________________________

## 4.2. Tips for using Git Bash 

** For instructions below using Git Bash, lines without "-" bullet points are commands that should be typed or copied into Git Bash.

- How to open Git Bash:
    - Open the Git Bash app through the Start Menu >> You will then need to navigate to the desired folder (using "cd"; see below).
    - Or: In any File Explorer >> Right click in the desired folder >> In popup, click "Git Bash Here".
    - Or: Open VS Code >> click "Terminal" at top >> "New Terminal" >> In the view that pops up, on the right, click the dropdown arrow next to the plus sign >> "Git Bash".
        - You can set "Git Bash" to be the default terminal for VSC in the same dropdown by clicking "Select Default Profile" >> then "Git Bash" in the dropdown at the top.

- Note: When you open a Git Bash, you should expect to see two prompt lines: 
    <username>@<computername> MINGW64 <path/of/current/folder> (name of git branch if in a git repository)
    $ <cursor where you can type commands>

** To PASTE into Git Bash, use: Shift + Insert 
** To EXECUTE a command, press Enter. (A new prompt line should appear when previous command has finished running.)
** To REPEAT a previous command, on a new line, use the Up/Down arrow keys.
** To QUIT if there is too much text on the screen, type "q".

- Some Tips from https://www.git-tower.com/blog/git-bash/#basic-bash-commands 
    "Here are some essential commands you will use every day:
        pwd — displays the path of your current working directory;
        cd <directory> — navigates to specified directory;
        cd .. — navigates to parent directory;
        ls — lists directory contents;
        mkdir <directory> — creates a new directory"

________________________________________________________________________________

## 4.3. Git Setup 

- Identify your Home Directory:
    1. Open a Git Bash shell. 
    2. See where your "home directory" is (shown by symbol "~" in paths):
        $HOME
            - It should be "/h/" -- your "H" (Home) Drive.
            - Your "H" Drive backs up daily and so is the best place to save files "locally."
    3. If $HOME is Not the "H" Drive, please contact the NEHV coding team & ask what to do.

- Set up personal information in Git (so that Git commits are attributed to you correctly):
    1. Open Git Bash.
        1. Drag it open wider & taller (or expand full screen).
    2. Check config settings (so can compare to settings after changes made):
        1. Execute:
            git config --list --show-origin
        - Expect to see about a dozen lines.
        ** Tip: If it glitches & you seem stuck >> type "q" (for "Quit").
    3. Set name & email for user on computer: 
        1. Execute these one-by-one, replacing <your_...> with what you want to be seen in Git commits (you do need the quotation marks in the commands):
            git config --global user.name "<your_name>"
            git config --global user.email "<your_email>"
        - Note: These commands will create a ".gitconfig" file in your $HOME directory.
    4. Check that it worked (compare to setting snapshot made before changes):
        1. Execute:
            git config --list --show-origin
        - Note: It's the same command as before >> you can use the up arrow to find it.
        - You should see new lines at the bottom for "file:H://.gitconfig" for "user.name" & "user.email".

________________________________________________________________________________

# 5. Clone the Code Repository 

- The code is saved in a remote Git repository on the Shared Drive here: "U:/Working/nehv_ds_code_repository/nehv_ds_code_repository.git" 
    - However, the .git folder only contains compressed snapshots of our files, so the code cannot be accessed directly there.
    - With Git you need to copy (or "clone") the repository to your "local" computer to access the code.

1. Prepare the location for your local repository:
    1. Open a Git Bash shell. 
    2. Create a "git" folder in your home directory:
        1. Go to $HOME location:
            cd ~ 
        2. Create new folder "git" there:
            mkdir git 
        3. Check that folder was created by listing what is in the current directory:
            ls

2. Clone the repository:
    1. In a Git Bash shell >> Navigate to where on your "local" machine you want to save the repository:
        cd ~/git
    2. In the desired folder, clone the remote repository:
        git clone "U:/Working/nehv_ds_code_repository/nehv_ds_code_repository.git" 

3. Review the code files:
    - On a Windows machine, your local code repository should now be here: 
        H:/git/nehv_ds_code_repository

________________________________________________________________________________

# 6. VS Code Setup 

- Visual Studio Code (VS Code or VSC) will be our Python IDE (integrated development environment) where we will interact with the code.
    - Resources:
        - General VS Code Documentation: 
            https://code.visualstudio.com/docs
            https://code.visualstudio.com/docs/getstarted/tips-and-tricks
        - Using Python in VS Code:
            https://code.visualstudio.com/docs/languages/python
            https://code.visualstudio.com/docs/python/python-quick-start
            https://code.visualstudio.com/docs/python/python-tutorial

But let's start at the beginning!

- Open VS Code: 
    - Open the VS Code app through the Start Menu.
    - or: In any File Explorer >> Right click in a folder or on a file >> In popup, click "Open with Code".
    - or: In File Explorer, in the folder for a code repository >> Open the ".code-workspace" file if it exists.
        - Go to the NEHV repository: H:/git/nehv_ds_code_repository
        - Open "OPEN-ME.code-workspace".

- If a popup asks "Do you trust the authors of the files in this folder?"
    - Click: "Yes, I trust the authors"

- If you are still reading these instructions from the file in the Shared Drive, re-open this instruction file in your local repository:
    1. Close any open VS Code windows.
    2. In File Explorer, go to the NEHV repository: H:/git/nehv_ds_code_repository 
    3. Double click to open "OPEN-ME.code-workspace".
    4. In VS Code >> In the upper-left, click the "Explorer" icon (image of files/papers) to see all files in folder.
    5. In the Explorer pane, double click "_INSTRUCTIONS_for_local_setup.md" to open this file in main viewer.

________________________________________________________________________________

## 6.1. Adjust Basic VSC Settings 

1. Open VS Code Settings:
    - File >> Preferences >> Settings
    - or: Click Gear in lower left >> Settings
    - or: shortcut: Ctrl + , (comma) 
2. Opt out of Microsoft collecting data:
    1. In Settings, search for "telemetry".
    2. Turn all telemetry/data collection OFF.
    - Reference: https://code.visualstudio.com/docs/supporting/faq#_how-to-disable-telemetry-reporting
- Tip: See all settings you have modified:
    - In VS Code Settings, on the right of the search bar at top >> Click the funnel icon to Filter Settings >> Click "Modified".

________________________________________________________________________________

## 6.2. Install VSC Extensions

1. In VS Code, on the left, click the icon of 4 boxes to see Extensions.
2. Search for and install the following:
    - Python-related:
        1. "Python" 
            - It will also install: 
                - "Pylance"
        2. "Jupyter" 
            - It will also install:
                - "Jupyter Cell Tags"
                - "Jupyter Keymap"
                - "Jupyter Notebook Renderers"
                - "Jupyter Slide Show"
        3. "autoDocstring - Python Docstring Generator"
    - Git-related:
        5. "Git Graph"
        - (optional) "GitLens — Git supercharged" 
    - General Coding:
        6. "IntelliCode"
            - It will also install: 
                - "IntelliCode API Usage Examples" 
        7. "Code Spell Checker"
        8. "Go to Character Position"
        9. "indent-rainbow"
        10. "Path Intellisense"
        11. "Prettier - Code formatter"
        12. "Rainbow CSV"
        13. "Todo Tree"
        14. "Material Icon Theme"
            - Setup:
                1. In Extension sidebar >> Click on Material Icon Theme >> A window describing it should open in the main viewer.
                2. At the top >> click "Set File Icon Theme" >> In the drop down at the top, click "Material Icon Theme".
                - Change themes in future: Gear in bottom left >> Themes.
        15. "Outline Map"
            - Setup: Open VS Code Settings (Ctrl+,). Search for the following settings:
                1. "outline-map.follow"             -- Change to "viewport"
                2. "outline-map.region.startRegion" -- Change to: >>>
                3. "outline-map.region.endRegion"   -- Change to: !>>>
                4. "outline-map.region.tag"         -- Change to: <>
            - Use: On the left, click the map icon >> It will open a side bar outline you can click through.
                - If Outline Map does not seem right, click back & forth into a different sidebar until Outline Map updates.

________________________________________________________________________________

## 6.3. Basic VS Code Use 

- Tip: Updating VS Code:
    - It is good to regularly check for VSC updates:
        - For VS Code itself: Click Gear in lower left >> "Check for Updates"
        - Extensions: In Extensions tab, an update notice should appear for an extension when it is needed.

- Tips: Features of Explorer pane:
    - At top: Shows all files & subfolders.
        - Note: If it does not seem to be showing everything, search in Settings for "explorer.excludeGitIgnore" & UN-select it.
    - Outline: Clickable outline for file; different for every file type.
        - These instructions are formatted to work with the Outline. Check it out!
    - Timeline: Shows history of Git commits (saved points in time).
        - We will discuss more below.

- General VS Code Tips:
    - VS Code is awesome for all types of work, not just coding. Examples:
        - Find & replace text in a file:
            - In a file press "Ctrl+F" (Find) or "Ctrl+H" (Replace). 
            - You can select text first & that text will be searched for.
            - More detail: https://code.visualstudio.com/docs/editor/codebasics#_find-and-replace 
                - https://code.visualstudio.com/docs/getstarted/tips-and-tricks#_search-and-modify 
        - Search & replace text in all files in a folder:
            - On far left, click the magnifying glass icon for the Search view.
            - More detail: https://code.visualstudio.com/docs/editor/codebasics#_search-across-files 
        - Side-by-side file editing:
            - You can open multiple files and drag them next to each other to see multiple at the same time.
            - More detail: https://code.visualstudio.com/docs/getstarted/userinterface#_side-by-side-editing 
        - Compare files:
            1. Go to the Explorer view.
            2. Left click one file (so that it opens in main viewer).
            3. Hold down "Ctrl" & Left click a second file (it will be highlighted but will not open).
            4. Right click either of the selected files.
            5. In popup, click "Compare Selected".
            - A side-by-side comparison will open. You can edit & save either file with this comparison still open.
            - This is super useful when two files seem similar and you want to see exactly where the differences are.
            - Tip: Open a second window for text comparison:
                1. In VS Code >> Go to "File" at top >> Click "New Window".
                2. In new window >> Under "File", create 2 New Text Files.
                3. Open the Explorer view >> Click the 3 dots to the right of "Explorer" >> Click "Open Editors".
                4. Use the instructions above to compare these 2 blank files.
                5. Now you can copy into either side any text you want to compare.
                - Tip: If you close all other VS Code windows first before you close this second comparison window, when you open up VS Code in the future, that second comparison window will also open -- ready for use.
            - More detail: https://code.visualstudio.com/docs/sourcecontrol/overview#_viewing-diffs 
        - Moving around a file:
            - Select every instance of selected text:     Ctrl + F2 
            - Put cursor at end of every selected line:   Shift + Alt + i 
            - Jump to matching parenthesis/bracket:       Ctrl + Shift + \ (backslash)
            - Select all of current line:                 Ctrl + L 
        - More Keyboard Shortcuts:
            - "File" >> "Preferences" >> "Keyboard Shortcuts"
            - https://code.visualstudio.com/shortcuts/keyboard-shortcuts-windows.pdf 
            - https://code.visualstudio.com/docs/getstarted/keybindings 

________________________________________________________________________________

# 8. Use Git in VS Code 

*** TODO: Will add notes to this section.

- Connect to Git repository:
    - When you open a Git repository folder in VS Code for the first time, VSC may ask if you want to connect to the repository >> Say Yes.
    - You can tell if you are connected to Git repository:
        - TODO: bottom left: main,
            Git Graph & see
            Git icon it's not asking you to connect -- 
            test -- by edit & saving any file: syou should see icon change in Explorer & an entry in Git
                see side-to-sed
        - TODO: Add HOW to commit. & how to cancel.
        - Explain staging, then committing
    - If you can't seem to access Git, you can adjust whether VSC automatically opens repositories in Settings by searching for setting "git.openRepositoryInParentFolders".

## Nebraska Team Policies for Git Usage:
    - Minimum Basic Version Control: If you make any changes, (1) make a commit & (2) push those changes to the remote repository.
    - Everyone working with NEHV code should talk and agree on team Git policies, including when & how to create/merge branches.

If you are just view:
 - git pull:
    click circle >> ok

THEN popup:
"Do you want git fetch to regularly check....."
check settings
TODO:

If you are making edits"
## General Git workflow:
    1. First, "pull" down from the remote repository changes ("commits") made by other people.
    2. Edit files in local repository.
    3. Pull down changes again! (in case someone else made changes since you last checked)
    4. Test that the code still works with your new changes.
    5. Save a snapshot of the repository by "committing" your changes.
    6. Send ("push") your changes (your "commits") to the remote repository.

- Please ask the coding team lead for a brief training if you have any further questions about Git after this.

- Resources:

    - Learn how to use Git:
        - Fast Git overview: https://www.freecodecamp.org/news/learn-the-basics-of-git-in-under-10-minutes-da548267cc91/ 
        - Interactive Git training: https://learngitbranching.js.org/
        - Git Documentation: https://git-scm.com/docs/gittutorial , https://git-scm.com/docs/user-manual 

    - Using Git with VS Code:
        - Overview with videos: https://code.visualstudio.com/docs/sourcecontrol/overview 

    - Staging and committing code changes: 
        - After you make a group of changes to files, save a snapshot of the repository for version control:
            https://code.visualstudio.com/docs/sourcecontrol/intro-to-git#_staging-and-committing-code-changes 
            https://code.visualstudio.com/docs/sourcecontrol/overview#_commit 

    - Pull and push changes: 
        - "Pull" down changes from the remote repository and send ("push") your changes up to the remote repository:
            https://code.visualstudio.com/docs/sourcecontrol/intro-to-git#_pushing-and-pulling-remote-changes 
            https://code.visualstudio.com/docs/sourcecontrol/overview#_git-status-bar-actions 

    - Using branches: 
        - The "main" branch (the "trunk" of the Git tree) is the production code that should always work & never break.
        - For significant changes to the code, create a side "branch" -- a parallel copy of all the code.
        - "Merge" your side branch back into the main branch after it is fully tested. 
            - Merges usually need review and approval.
        https://code.visualstudio.com/docs/sourcecontrol/intro-to-git#_using-branches 
        https://code.visualstudio.com/docs/sourcecontrol/overview#_branches-and-tags 

    - Visual map of Git commits and branches:
        - Extension "Git Graph":
            - If installed & if you are in a git repository in VS Code >> in bottom left >> Click "Git Graph".

________________________________________________________________________________

# 9. Manage Python with Conda 

- Conda is an environment management system installed with Anaconda that we will use to install and manage Python.
- We will use a Conda environment to manage the version of Python we want.

________________________________________________________________________________

## 9.1. Information about Managing Python 

- Python versions:
    - Need at least Python 3.10 (match-case statements released in 3.10). 
    - See the status of Python versions: https://devguide.python.org/versions/ 
    - See released versions: https://www.python.org/doc/versions/ 

- Our Plan: 
    - When we update, we will update Python to the highest stable version & will keep the Pipfile up-to-date (explained below).
    - We will monitor Python security alerts to update as needed.

- (for code team leads) Sign up for Python security alerts:
    https://www.python.org/dev/security/#:~:text=Published%20advisories%20and%20mailing%20list 
    https://mail.python.org/mailman3/lists/security-announce.python.org/

- Conda documentation: https://docs.conda.io/projects/conda/en/stable/ 
    - Conda commands cheatsheet: https://docs.conda.io/projects/conda/en/stable/user-guide/cheatsheet.html 
        - To learn more about any Conda command: In Git Bash, add "--help" after the command & run it. For example:
            conda install --help 
    - Creating environments: https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html 
    - Managing Python: https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-python.html 
        - To see which versions of Python are available through Conda: Run in Git Bash:
            conda search --full-name python 

________________________________________________________________________________

## 9.2. Conda Installation with Miniconda 

- Anaconda is an application that manages coding languages and code packages.
    - Why use Anaconda? It can manage our Python version & related paths.
    - Anaconda installation includes Conda, Spyder IDE, Jupyter Notebook, etc.
    - Documentation: https://docs.anaconda.com/free/navigator/ 

- Note: At the moment, the only reason we use Anaconda is to install Conda. 
    - Sometime soon we will switch to use Miniconda instead because... 
        (1) Miniconda takes much less memory (about 600 MB compared to 6+ GB for Anaconda) and 
        (2) we are no longer using Anaconda features we thought we would (like environment setup through the GUI & the Spyder IDE).
    - Miniconda documentation: https://docs.anaconda.com/free/miniconda/ 


1. Installing Miniconda at this link: https://www.anaconda.com/download/success
    1. Choose the Miniconda Installer option and select the Windows 64-but Graphic Installer
    2. Click on the downloaded installer Miniconda3-latest-Windows-x86_64.exe
    3. This will bring up a the install window 
        1. Click "Next"
        2. Select "I Agree"
        3. Install For: "Just Me (recommended)"
        4. Install folder "C:\Users\<Your UserName>\AppData\Local\miniconda3"
        5. Advanced Installation Options:
            - SELECT "Create Shortcuts"
            - SELECT "Clear the package cache upon completion"
            - SELECT "Register Miniconda3 as my default Python 3.13"
            - Install
        6. You could get an "Install complete" window. Click "Next" 
        7. Click "Finish"
    
1. Miniconda should be installed on your system and you can now use Python
    - HOWEVER, if not on workspace, please let the NE Coding team know!

________________________________________________________________________________

## 9.3. Conda Setup 

Set up Conda for use.


1. Try to go to your Desktop settings and edit your Environment Path variables using your NE password. If you do not have Admin privileges Email NE helpdesk (service.desk@nebraska.gov) and ask that the following paths be added to your  Environment variables:

        C:\Users\<your-username>\AppData\Local\miniconda3

        C:\Users\<your-username>\AppData\Local\miniconda3\Scripts
    Note: This step may be uncessary if you're able to setup .bashrc without it. 

2. "Add the conda shell script to your .bashrc":
    1. Open a new Gitbash session from your HOME directory and type 
    "/c/Users/YourUserName/AppData/Local/miniconda3/Scripts/conda.exe init bash"
    2. You can open the file by running "notepad ~/.bashrc"
     -It should contain code that looks like this 
     # >>> conda initialize >>>
        __conda_setup="$('/c/Users/YourUserName/AppData/Local/miniconda3/Scripts/conda.exe' 'shell.bash' 'hook' 2> /dev/null)"
        if [ $? -eq 0 ]; then
            eval "$__conda_setup"
        else
            if [ -f "/c/Users/YourUserName/AppData/Local/miniconda3/etc/profile.d/conda.sh" ]; then
                . "/c/Users/YourUserName/AppData/Local/miniconda3/etc/profile.d/conda.sh"
            fi
        fi
        unset __conda_setup
    # <<< conda initialize <<<

    3. Apply the updated bash by running "source ~/.bashrc"
    4. Run:
        - "conda --version"
        - "conda activate base"
    If you see Conda's version and the prompt changes (e.g. (base) appears), you’re done! 

________________________________________________________________________________

## 9.4. Create a new Conda virtual environment 

- Note: You can create Conda environments through the Anaconda Navigator app; however, only certain versions of Python are available through that way. So we will use Conda commands to get the version we want.

1. Open Git Bash.
2. Verify that Conda is running and check version: Run:
    conda info 
3. Create a new Conda virtual environment with a specific version of Python (it also installs certain basic packages like Pip): Run:
    -conda create -n conda_env_py3121 python=3.12.1
    -if asked if you accept Terms of Service, enter "a"


________________________________________________________________________________

## 9.5. Manage Conda Environments 

- In the future, to update the version of Python, you will need to create a new virtual environment with the steps above. When creating the environment, change the "_py###" part of the name and the "python=###" part of the command. For example, when version 3.12.2 comes out, the command would be (run in Git Bash outside of any Conda environment):
    conda create -n conda_env_py3121 python=3.12.1 

- To see all conda environments: Run in Git Bash outside of any Conda environment:
    conda env list 

- To remove test/old conda environments: Run in Git Bash outside of any Conda environment:
    conda remove -n NAME_OF_ENVIRONMENT --all 

________________________________________________________________________________

## 9.6. Test the Conda Environment 

1. Open a Gitbash Terminal in your VsCode git repository by clicking Terminal in the left hand corner, and selecting "Git Bash" from the drop down. In Git Bash terminal, enter the Conda virtual environment: Run:
    conda activate conda_env_py3121 
        - Now in Git Bash, above the first prompt line, you should see the name of the conda environment in parentheses: (conda_env_py3121) 
2. Check that Python & Pip are installed inside the Conda environment: Run each of these:
    python --version 
    pip --version 
        - Pip is the basic Python package manager.
3. See the path to the version of python being used. In the output, the first path in the list is the one being used; the others are just other versions of python on your machine. Run:
    where python 
3. Check which packages are installed inside the Conda environment: Run each of these:
    pip list 
    conda list 
        - These lists will be different because conda looks at more things than pip.
4. To exit a Conda environment: Run:
    conda deactivate 

________________________________________________________________________________

# 10. Manage Python Packages with Pipenv -- NOTE: I'm NOT currently using Pipenv, I just use pipreq which is described at the end, so all of step 10 can be skipped. These instructions should still work if deciding to use pipenv though. 

Pipenv is a Python package and the environment and package manager we will use to manage our Python packages.
    - Pipenv creates 2 files to manage packages ("Pipfile" & "Pipfile.lock") that we can save with Git.

- Pipenv documentation:
    - https://pypi.org/project/pipenv/ 
    - https://pipenv.pypa.io/ 
    - Pipenv commands: https://pipenv.pypa.io/en/latest/commands.html 
        - To learn more about any Pipenv command: In Git Bash, add "--help" after the command & run it. For example:
            pipenv --help 
            pipenv install --help 

________________________________________________________________________________

## 10.1. Pipenv Installation 

1. Navigate to your local code repository (H:/git/nehv_ds_code_repository) in Windows File Explorer. Open the folder in VSCode
2. Open Git Bash Terminal in VS Code by Right Clicking on the Terminal section at the top of your window, and selecting "Git Bash"
3. In Git Bash Terminal, enter the Conda virtual environment: Run:
    conda activate conda_env_py3121 
4. Install the package "pipenv": Run:
    pip install pipenv 
    Note: conda installation of pipenv says pipenv is not supported for Python version 3.12 use an earlier version of python and install with conda, or use pip
5. Check that pipenv installed. Check the version or check that it's in the list. Run any of these: 
    pipenv --version 
    pip list 
    conda list 

________________________________________________________________________________

## 10.2. Install Python Packages 

1. Open the repository in VS Code and open a  Git Bash terminal (if not already open).
2. Enter the Conda virtual environment (if not already in it):
    conda activate conda_env_py3121 
        - Reminder: You can tell if you are in the Conda environment if you see the name of the environment in parentheses above what is normally the first prompt line.

3. In the main folder of the repository, check that files "Pipfile" & "Pipfile.lock" exist by running:
    ls 
        ** If you do not see both files listed in the main repository folder, please stop & contact the NE Code Team!
4. Identify the specific Python patch version (3 numbers separated by two periods) being used by the Conda environment: Run: 
    python --version  

5. Install packages as specified in "Pipfile": Run: 
    pipenv install --python 3.12.1 
        - ** Make sure you install with exactly the same Python patch version as is used by the Conda environment. 
        - If "Pipfile" has been updated since packages were last installed, when "pipenv install" is run, it will update "Pipfile.lock". See section below about updating packages or the Pipfile.

________________________________________________________________________________

## 10.3. Test the Pipenv environment 

- From inside the Conda virtual environment (see above):
    - (Optional) Exploration:
        - Locate project folder: Run:
            pipenv --where 
        - Locate virtual environment files: Run:
            pipenv --venv
        - Locate Python interpreter file: Run:
            pipenv --py
        - Display dependency graph of currently-installed packages: Run:
            pipenv graph 
                - Note: Sometimes "graph" is glitchy. It's okay if it doesn't work. Can use the following if normal way does not work:
                    pipenv graph --json-tree 

- From inside the Conda virtual environment:
1. Enter the Pipenv virtual environment: Run: 
    pipenv shell
        - Note: If you get error "UNC paths are not supported", try resetting the path:
            cd ~/git/nehv_ds_code_repository 
            - ... and then trying "pipenv shell" again.
3. (optional) Check that you are in a Pipenv environment:
    - Looking at the Git Bash prompt, you cannot tell that you are in a Pipenv environment.
    - You can check that you are in the expected virtual environment by running:
        $VIRTUAL_ENV 
            - You should see something like: "bash: <path_to_expected_venv>: Is a directory"
2. Check the version of Python:
    python --version 
        ** If this version does not match the expected version, please contact the NE Coding Team!
4. Check that all required packages (listed in "Pipfile") are installed in the Pipenv virtual environment. Run:
    pip list 
5. Leave the Pipenv environment: Run:
    exit 
        - Caution: If you run "exit" in a Conda environment or in the original base Bash environment, it will close Git Bash.

________________________________________________________________________________

## 10.4. Debugging errors with Pipenv setup (if needed) 

- If there are errors while setting up Pipenv, here are a few things to try before re-trying what you were doing:
    1. Close & reopen Git Bash.
    2. Clear the Pipenv cache.
        1. Open Git Bash >> Activate the Conda environment >> From inside the Conda virtual environment: Run:
            pipenv --clear 
    3. Remove the Pipenv virtual environment (see below) & redo instructions.
    4. If there is a memory/space issue on your machine: Clean temp files & retry instructions:
        1. Open Windows Settings >> "System" >> "Storage".
        2. After the bars appear, click "Temporary files" >> You can clean out some files here.
    5. If none of these work, please contact the NE Code Team.

- Remove the Pipenv virtual environment & recreate it:
    1. Delete the "Pipfile.lock" file (if you want to regenerate it from the "Pipfile").
    2. Remove the Pipenv virtual environment (venv):
        1. Open Git Bash >> Activate the Conda environment >> From inside the Conda virtual environment: 
            1. (optional) Before removing the venv: Locate the venv files (if you want to check after removing the venv to see if the files are gone): Run:
                pipenv --venv
            2. To remove the venv: From inside the Conda virtual environment: Run:
                pipenv --rm 
    3. Clear the Pipenv cache (see above).
    4. Recreate the Pipenv virtual environment & reinstall Python packages per instructions above.

________________________________________________________________________________

## 10.5. Python Packages we will use 

- See list of Python packages needed for this project in the "Pipfile" in the code repository (H:/git/nehv_ds_code_repository).
    - Note: In the Pipfile, all package names are lowercased.

- Packages for data analysis:
    - "pandas"
        - https://pandas.pydata.org/ 
        - https://pypi.org/project/pandas/
        - See release notes: https://pandas.pydata.org/docs/whatsnew/index.html 
        - We need Pandas 2.0+ -- compare() fixed in 2.0.0.
    - "numpy" 
        - https://numpy.org/ 
        - https://pypi.org/project/numpy/ 

- Packages needed for running Python interactively in VS Code:
    - "ipython" https://pypi.org/project/ipython/ 
    - "ipykernel" https://pypi.org/project/ipykernel/ 

- Packages used by pandas for Excel files, but not always automatically installed:
    - "XlsxWriter" https://pypi.org/project/XlsxWriter/ 
    - "openpyxl" https://pypi.org/project/openpyxl/ 
    - Pandas documentation about these packages:
        - https://pandas.pydata.org/docs/user_guide/io.html#excel-writer-engines 
        - https://pandas.pydata.org/pandas-docs/stable/getting_started/install.html#optional-dependencies 

________________________________________________________________________________

## 10.6. Process to update project packages 

- To add a new package to the project:
    1. Inside the Conda environment (conda activate conda_env_py3121), but outside the Pipenv shell: Run "pipenv install" followed by the package names. For example:
        pipenv install pandas numpy 
            - This should update both the Pipfile & the Pipfile.lock. In the Pipfile, you should see a new line like: ipkernel = "*" 
            - Alternatively, you could manually add the package to the Pipfile & run "pipenv install".
    2. Make sure to Git commit any updates to the Pipfile or Pipfile.lock.

- To update a package to a specific version:
    1. In the Pipfile, edit the version required for the package from * to a phrase using ">=". For example: pandas = ">=2.*"
    2. Then in the Conda environment, run "pipenv install".
    3. Make sure to Git commit any updates to the Pipfile or Pipfile.lock.
    - More examples about specifying a package version:
        - https://pipenv.pypa.io/en/latest/specifiers.html
        - https://peps.python.org/pep-0440/#version-specifiers

- To update the version of Python:
    - See section above called "Manage Conda Environments" & all instructions after that: 
        1. Will need to create a new Conda virtual environment...
        2. And then will need to create a new Pipenv virtual environment.
        3. Make sure to Git commit any updates to the Pipfile or Pipfile.lock.

________________________________________________________________________________

# 11. Setup your new virtual environment as your Python Interpreter in VSCode and install required packages to run code

1. Open the repository in VSCode (if not already open) and open a new Git Bash terminal. 

2. Activate your virtual environment. Run 
 - conda activate conda_env_py3121`

4. Open Command Palette with Ctrl+Shift+P

    Type: Python: Select Interpreter

    Choose: "Enter interpreter path..."

    (If this doesn't pop-up, try exiting out of VSCode and relaunching)

    Then: Enter
    C:\Users\YourUsername\AppData\Local\miniconda3\envs\conda_env_py3121\python.exe

5. Install required packages from Git Bash terminal with environment running:
  Note: pipreq will go through all files and find packages used in all scripts. If this has been done recently, you can skip to step 3 to install packages directly from requirements.txt
    1.  Install pipreqs for package management by running
        pip install pipreqs
    2.  Run 
        pipreqs . --force  
    3. Run 
        pip install -r requirements.txt
6. You should have the required packages installed needed to run the code, and can now run the NE MEICHV process.

If wanting to run in interactive window,  make sure ipykernel is installed in your virtual environment.

NOTE: You will need at LEAST 8 GB of RAM on your virtual machine in order to run this process, preferably 16 GB. 




How to change virtual environments in VSCode:
https://code.visualstudio.com/docs/python/environments#_select-and-activate-an-environment

Test that Variable Viewer & Data Viewer working.
See example of using them here: https://code.visualstudio.com/docs/datascience/data-science-tutorial#_prepare-the-data 


________________________________________________________________________________

# Conclusion 

Thank you for reviewing these instructions!
If you have any feedback, please let the NE Coding Team know!


