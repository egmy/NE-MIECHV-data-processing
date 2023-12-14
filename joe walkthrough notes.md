# Install Git:

    # Install Git after Visual Studio Code (during install will set VSC as Git's default editor).
    # Download: https://git-scm.com/
        # When installing, use these settings:
            # In Select Components: Click "Check daily for Git for Windows updates"
            # Instead of Vim, Set the editor to "Use Visual Studio Code as Git's default editor"
            # "Override the default branch name for new repositories": main
            # Use Recommended "Git from the command line and also from 3rd-party software".
            # In "Configuring the line ending conversions": Select "Checkout as-is, commit Unix-style line endings"
            # Git Credential Manager Core: https://github.com/microsoft/Git-Credential-Manager-Core/blob/main/docs/faq.md#about-the-project
    # After: When you open a Git Bash it should use MINGW64.


    WHich version of Git? Downloaded 2.41.0
    - I'd prefer a step by step of each page
    I don't know what the # After: When you open a Git Bash it should use MINGW64 means

# Set up personal information in Git:

    # Open Git Bash.
    # See config settings:
        # git config --list --show-origin
    # Set name & email for user on computer:
        # git config --global user.name "{insert_my_name}"
        # git config --global user.email "{insert_my_email}"
    # Check that it worked:
        # git config --list --show-origin


How do I check the last # Check that it worked:?
- What am I looking for?
- How long should it run? Never fully completed

# Install VSC Extensions:

    # Python-related:
        # "Python" (also installs other Python & Jupyter extensions)
            # "Pylance"
            # "isort"
            # "Jupyter" (installed by Python, installs other Jupyter extensions)
                # "Jupyter Keymap"
                # "Jupyter Notebook Renderers"
                # "Jupyter Slide Show"
                # "Jupyter Cell Tags"
        # "autoDocstring"
        # "Python Environment Manager"

# "Jupyter" (installed by Python, ...)

- Jupyter doesn't seem to have install by Python
- I installed separately

# "autoDocstring"

- labeled "autoDocstring - Python Docstring Generator"
- installed separately

# "Python Environment Manager"

- also installed separately

  # General Coding:

        # "IntelliCode"
            # "IntelliCode API Usage Examples" (installed by IntelliCode)
        # "Path Intellisense"
        # "indent-rainbow"
        # "Rainbow CSV"
        # "Prettier"
            # Note: Ctrl + Shift + P >> Format Document; (OR: Ctrl + Alt + F)
        # "Code Spell Checker"
        # "Todo Tree"

  # Git-related:

        # "GitLens — Git supercharged"
        # "Git Graph"

# "Prettier"
- labeled "Prettier - Code formatter" 
    # Note: Ctrl + Shift + P >> Format Document; (OR: Ctrl + Alt + F) 
    - I don't know what this means - do it now during install



# Connect to Git project:
    !!!!!!!!! check
    # VSC then may ask if you want to connect to the repository >> Yes. (Which setting is this?)
- how do I do this?



# Create ".bashrc" file in your HOME directory:
    # create new TXT file, rename file & extension to be: ".bashrc".
- what does this actually do? the steps below created a new .bashrc with directory in it



# "Add the conda shell script to your .bashrc":
    # From these instructions: https://discuss.codecademy.com/t/setting-up-conda-in-git-bash/534473
    # Key instructions summarized here:
        # "use the search bar and search for “Anaconda3” and scroll down to the folders"
            # If doesn't show up, probably this: "C:\ProgramData\Anaconda3".
        # Open the "Anaconda3" folder >> navigate to "etc" >> then "profile.d"
            # For example: "C:\ProgramData\Anaconda3\etc\profile.d".
        # "Inside of the profile.d folder, you should see a file called conda.sh." 
        # Close any open Git Bash
        # In the "profile.d" folder >> Right Click >> choose "Git Bash Here".
        # In Git Bash that opens, run:
            # echo ". '${PWD}'/conda.sh" >> ~/.bashrc 
        # Close Git Bash.
        # Can go check the "~/.bashrc" file. New line should have been created, for example:
            # echo . '/c/ProgramData/Anaconda3/etc/profile.d'/conda.sh 

# search bar stuff already in C:\ProgramData\Anaconda3
- confusing to search bar - takes forever

# Can go check the "~/.bashrc" file. 
- asked if wanted to open untrusted file -yes
- not sure what this does



######################################
### Set up Conda Environment ### 
#######################################

# First, create new environment: conda_env_nebraska_py310.
    # NEED Python 3.10+ -- match-case statements released in 3.10. 
    # See https://devguide.python.org/versions/ for status of python versions.
    # See release versions: https://www.python.org/doc/versions/ 

# Instructions if using Anaconda Navigator:
    # Open Anaconda Navigator >> Environments (on left) >> Create (button at bottom)
    # Name environment & select version of Python.
        # Note: Only certain versions of Python available through this way. If what's desired is not available, do the following:

# To get a specific version of Python, use a Bash Prompt. (installed along with Git).
    # Open Bash Prompt.

# Test that conda is running ### "verify conda install and check version":
conda info

# Create new conda virtual environment with a specific version of Python:
    # Also installs certain basic packages like pip.
conda create -n conda_env_nebraska_py310 python=3.10

- Not sure what this entire section means? where am I creating a new environment? Need more instructions please

- Anaconda Navigator opens a 


### Nathan: Would be good to add more context on why we are taking steps. Setting up Conda Environments didn't make sense. Need more clarity.

