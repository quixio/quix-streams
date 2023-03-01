# Installing on Quix Streams on a M1/M2 Mac:

Rosetta amd64 emulation is currently required for Apple silicon (M1 and M2-based) Macs

To install Quix Streams on M1/M2 Macs, follow these steps: 

1. To make sure you have Rosetta installed, open Mac Terminal, and run the command `softwareupdate --install-rosetta`.

2. If you don't have Brew installed, install it using the instructions on the [Brew homepage](https://brew.sh). Make sure that after the install script runs, that you perform any configuration recommended by the script.

3. Install an additional terminal, such as iTerm2. You could do this, for example, with `brew install iterm2`.

4. Open finder, go to Applications, and locate iTerm2.

5. Right-click the iTerm2 application icon to display the context-sensitive menu, and select the menu item `Duplicate`.

6. Rename the duplicate created to `iTerm2 rosetta`.

7. Right-click `iTerm2 rosetta` again and select `Get Info`.

8. Tick the `Open using rosetta` checkbox, so that iTerm2 is always opened using Rosetta.

9. Launch `iTerm2 rosetta` by double-clicking it.

10. On the command line, run the `arch` command. This will display `i386`. If not, check your steps so far.

11. Install Brew again. This installs a new copy of Brew to a new directory structure for i386 (x86_64).

12. Open your Zsh profile file, `~/.zprofile`, using a text editor such as Nano. For example, with the command `nano ~/.zprofile`.

13. Add the following code to `~/.zprofile`:

    ```
    if [ $(arch) = "i386" ]; then
        PATH="/usr/local/bin/brew:${PATH}"
        eval "$(/usr/local/bin/brew shellenv)"
    fi
    ```

    This will ensure that when you open `iTerm2 rosetta`, your `brew` command will point at the correct (x86_64) Brew installation.

14. Reload your Zsh profile by running `source ~/.zprofile`, or opening a new instance of `iTerm2 rosetta`.

15. Install Python with the command `brew install python3`.

16. Using log messages from `brew`, check where Python was installed, for example: `/usr/local/Cellar/python@3.10/3.10.9/bin/python3`. If not sure, check with `ls /usr/local/Cellar`.

17. Open your `~/.zprofile` file again, and add the following line inside the `if` statement:

    ```
    if [ $(arch) = "i386" ]; then
        PATH="/usr/local/Cellar/python@3.10/3.10.9/bin:${PATH}"
        ...
    fi
    ```

18. Reload your Zsh profile by running `source ~/.zprofile`, or by starting a new instance of `iTerm2 rosetta`.

19. Install Quix Streams:

    ```
    python3 -m pip install quixstreams --user
    ```

20. You can now run your code that uses Quix Streams:
    
    ```
    python3 yourcode.py
    ```

You have now successfully installed Quix Streams on M1/M2 architectures.