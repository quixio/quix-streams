#!/bin/zsh

_=$(python3 -m pip show wheel)
if [[ $? != 0 ]]; then
	python3 -m pip install wheel
fi

_=$(dotnet --version)
if [[ $? != 0 ]]; then
	curl -L https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
	chmod +x ./dotnet-install.sh
    if [ $(arch) == "i386" ]; then
        ./dotnet-install.sh --channel 7.0.1xx
    else
        ./dotnet-install.sh --version 8.0.100-alpha.1.23055.13 --channel 8.0.1xx
    fi
	echo "export DOTNET_ROOT=$HOME/.dotnet" >> ~/.zshrc
	echo "export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools" >> ~/.zshrc
	source ~/.zshrc
fi

brewInstallLoc=''
brewInstalled=false
if [[ `uname -m` == 'arm64' ]]; then
    brewInstallLoc='/opt/homebrew/bin'
    if [[ -d $brewInstallLoc ]]; then
        brewInstalled=true
    fi
else
    brewInstallLoc='/usr/local/bin'
    if [[ -f /usr/local/bin/brew ]]; then
        brewInstalled=true
    fi
fi
if [[ "$brewInstalled" = true ]]
then
    case ":$PATH:" in
    *:$brewInstallLoc:*) : ;;
    *) echo "export PATH=$brewInstallLoc:$PATH" >>  ~/.zshrc && source  ~/.zshrc ;;
    esac
    brew update
    if [ $? != 0 ] ; then exit 1 ; fi ;
else
    # Install Homebrew
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    if [ $? != 0 ] ; then exit 1 ; fi ;
    case ":$PATH:" in
    *:/opt/homebrew/bin:*) : ;;
    *) echo "export PATH=$brewInstallLoc:$PATH" >>  ~/.zshrc && source  ~/.zshrc ;;
    esac
fi

brew install librdkafka
